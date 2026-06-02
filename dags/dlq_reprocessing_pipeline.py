"""
DAG : dlq_reprocessing_pipeline
==================================
Retraite périodiquement les événements défectueux de la Dead Letter Queue.

Planification : toutes les heures
Catchup       : désactivé
"""

from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES      = 3
BATCH_SIZE       = 100


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        """Récupère les événements en attente de retraitement."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT id::text, payload, error_type, retry_count, original_topic
            FROM dead_letter_events
            WHERE status = 'pending'
              AND retry_count < %s
            ORDER BY created_at ASC
            LIMIT %s
        """, (MAX_RETRIES, BATCH_SIZE))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        events = [
            {
                "id": row[0],
                "payload": row[1],
                "error_type": row[2],
                "retry_count": row[3],
                "original_topic": row[4],
            }
            for row in rows
        ]
        logger.info("%d événements pending trouvés", len(events))
        return events

    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        """Tente de corriger et réinjecter chaque événement défectueux."""
        if not pending_events:
            return {"reprocessed": [], "failed": []}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Charger les track_ids valides pour validation
        cursor.execute("SELECT id::text FROM tracks")
        valid_tracks = {row[0] for row in cursor.fetchall()}

        reprocessed = []
        failed = []

        for event in pending_events:
            try:
                payload = event["payload"] if isinstance(event["payload"], dict) else json.loads(event["payload"])
                can_fix = True
                fixed_payload = dict(payload)

                # user_id manquant → impossible à corriger
                if not payload.get("user_id"):
                    can_fix = False

                # track_id inconnu → abandonner
                if payload.get("track_id") and payload["track_id"] not in valid_tracks:
                    can_fix = False

                # timestamp invalide → fallback sur now
                if not payload.get("timestamp"):
                    fixed_payload["timestamp"] = datetime.utcnow().isoformat() + "Z"

                # duration_ms manquant ou invalide → fallback
                if not payload.get("duration_ms") or payload.get("duration_ms", 0) <= 0:
                    fixed_payload["duration_ms"] = 30000

                # completed manquant → fallback
                if "completed" not in fixed_payload:
                    fixed_payload["completed"] = False

                if can_fix and fixed_payload.get("user_id") and fixed_payload.get("track_id"):
                    reprocessed.append({"id": event["id"], "payload": fixed_payload})
                else:
                    failed.append({"id": event["id"], "retry_count": event["retry_count"]})

            except Exception as e:
                logger.error("Erreur retraitement event %s : %s", event["id"], e)
                failed.append({"id": event["id"], "retry_count": event["retry_count"]})

        cursor.close()
        conn.close()

        logger.info("Retraitement : %d succès, %d échecs", len(reprocessed), len(failed))
        return {"reprocessed": reprocessed, "failed": failed}

    @task(task_id="update_dlq_status")
    def update_dlq_status(results: dict, **context) -> dict:
        """Met à jour le statut des événements dans dead_letter_events."""
        reprocessed = results.get("reprocessed", [])
        failed = results.get("failed", [])

        if not reprocessed and not failed:
            logger.info("Aucun événement à mettre à jour.")
            return {"reprocessed": 0, "abandoned": 0, "pending": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Réinsérer dans listening_events et marquer comme reprocessed
        reprocessed_count = 0
        for event in reprocessed:
            payload = event["payload"]
            try:
                cursor.execute("""
                    INSERT INTO listening_events
                        (id, user_id, track_id, timestamp, duration_ms, completed, event_source)
                    VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    payload.get("user_id"),
                    payload.get("track_id"),
                    payload.get("timestamp"),
                    payload.get("duration_ms", 30000),
                    payload.get("completed", False),
                    payload.get("event_source", "dlq_reprocessed"),
                ))
                cursor.execute("""
                    UPDATE dead_letter_events
                    SET status = 'reprocessed',
                        resolved_at = NOW(),
                        last_retry_at = NOW()
                    WHERE id = %s
                """, (event["id"],))
                reprocessed_count += 1
            except Exception as e:
                logger.error("Erreur insertion event retraité %s : %s", event["id"], e)

        # Mettre à jour les échecs
        abandoned_count = 0
        pending_count = 0
        for event in failed:
            new_retry = event["retry_count"] + 1
            new_status = "abandoned" if new_retry >= MAX_RETRIES else "pending"
            cursor.execute("""
                UPDATE dead_letter_events
                SET retry_count = %s,
                    last_retry_at = NOW(),
                    status = %s
                WHERE id = %s
            """, (new_retry, new_status, event["id"]))
            if new_status == "abandoned":
                abandoned_count += 1
            else:
                pending_count += 1

        conn.commit()
        cursor.close()
        conn.close()

        stats = {
            "reprocessed": reprocessed_count,
            "abandoned": abandoned_count,
            "pending": pending_count,
        }
        logger.info("Bilan DLQ : %s", stats)
        return stats

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)