"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis (pub/sub),
les valide, les enrichit avec le catalogue et les stocke.

Planification : toutes les 5 minutes
Catchup       : désactivé (micro-batch temps réel)
"""

from datetime import datetime, timedelta
import json
import logging
import io

import boto3
import pandas as pd
import redis as redis_lib

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit et les stocke en dual : Parquet (MinIO) + PostgreSQL.

### Sources
- Redis LIST `listening_events_buffer`
- Redis LIST `p2p_network_events_buffer`

### Destinations
- Table `listening_events` (PostgreSQL)
- Fichiers Parquet partitionnés sur MinIO : `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` (pour les events invalides)

### Idempotence
Chaque event est identifié par `event_id` (UUID).
L'upsert utilise `ON CONFLICT (id) DO NOTHING` pour éviter les doublons.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

POSTGRES_CONN_ID  = "spotify_postgres"
REDIS_URL         = "redis://redis:6379/1"
MINIO_ENDPOINT    = "http://minio:9000"
MINIO_BUCKET      = "spotify-parquet"
BATCH_SIZE        = 1000  # max events à consommer par run


with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        """
        Consomme les événements depuis des Redis LISTs.
        Le simulateur publie sur des channels pub/sub, mais pour le batch
        on lit depuis des listes Redis (clés: listening_events_buffer, p2p_network_events_buffer).
        Si les listes sont vides, on retourne des listes vides sans erreur.
        """
        r = redis_lib.from_url(REDIS_URL, decode_responses=True)

        listening = []
        p2p = []

        # Lire jusqu'à BATCH_SIZE events depuis les listes Redis
        for _ in range(BATCH_SIZE):
            msg = r.rpop("listening_events_buffer")
            if msg is None:
                break
            try:
                listening.append(json.loads(msg))
            except json.JSONDecodeError:
                logger.warning("Event listening invalide (JSON) : %s", msg)

        for _ in range(BATCH_SIZE):
            msg = r.rpop("p2p_network_events_buffer")
            if msg is None:
                break
            try:
                p2p.append(json.loads(msg))
            except json.JSONDecodeError:
                logger.warning("Event p2p invalide (JSON) : %s", msg)

        logger.info("Consommé : %d listening, %d p2p", len(listening), len(p2p))
        return {"listening": listening, "p2p_network": p2p}

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """
        Valide les événements et envoie les invalides en DLQ.
        Champs obligatoires listening : event_id, user_id, track_id, timestamp, duration_ms
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        valid_listening = []
        valid_p2p = []
        errors = 0

        required_listening = ["event_id", "user_id", "track_id", "timestamp", "duration_ms"]

        for event in raw_events.get("listening", []):
            if all(k in event for k in required_listening) and event.get("duration_ms", 0) > 0:
                valid_listening.append(event)
            else:
                errors += 1
                cursor.execute(
                    """INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message)
                       VALUES (%s, %s::jsonb, %s, %s)""",
                    ("listening_events", json.dumps(event), "validation", "Champs obligatoires manquants"),
                )

        for event in raw_events.get("p2p_network", []):
            if all(k in event for k in ["event_id", "event_type", "peer_id", "timestamp"]):
                valid_p2p.append(event)
            else:
                errors += 1
                cursor.execute(
                    """INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message)
                       VALUES (%s, %s::jsonb, %s, %s)""",
                    ("p2p_network_events", json.dumps(event), "validation", "Champs obligatoires manquants"),
                )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Validation : %d valides listening, %d valides p2p, %d erreurs", len(valid_listening), len(valid_p2p), errors)
        return {"valid_listening": valid_listening, "valid_p2p": valid_p2p, "errors": errors}

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """
        Enrichit les événements d'écoute avec les métadonnées du catalogue PostgreSQL.
        Fait une seule requête SQL pour tous les track_ids.
        """
        events = validated.get("valid_listening", [])
        if not events:
            return []

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        track_ids = list({e["track_id"] for e in events})
        cursor.execute(
            "SELECT id, title, artist_id, genre FROM tracks WHERE id = ANY(%s)",
            (track_ids,)
        )
        rows = cursor.fetchall()
        track_map = {str(row[0]): {"title": row[1], "artist_id": str(row[2]) if row[2] else None, "genre": row[3]} for row in rows}

        enriched = []
        unknown = 0
        for event in events:
            track_info = track_map.get(event["track_id"])
            if track_info:
                enriched.append({**event, **track_info})
            else:
                unknown += 1
                cursor.execute(
                    """INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message)
                       VALUES (%s, %s::jsonb, %s, %s)""",
                    ("listening_events", json.dumps(event), "unknown_track", f"track_id inconnu : {event['track_id']}"),
                )

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Enrichissement : %d enrichis, %d inconnus", len(enriched), unknown)
        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """
        Sauvegarde les événements enrichis en Parquet sur MinIO.
        Partitionné par date et heure.
        """
        if not enriched_events:
            logger.info("Aucun event à sauvegarder en Parquet.")
            return "no_data"

        df = pd.DataFrame(enriched_events)
        now = datetime.utcnow()
        date_str = now.strftime("%Y-%m-%d")
        hour_str = now.strftime("%H")
        run_id = context["run_id"].replace(":", "-").replace("+", "-")

        key = f"listening_events/date={date_str}/hour={hour_str}/part-{run_id}.parquet"

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        s3.upload_fileobj(buffer, MINIO_BUCKET, key)
        logger.info("Parquet uploadé : s3://%s/%s", MINIO_BUCKET, key)
        return key

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """
        Insère les événements dans listening_events de façon idempotente.
        ON CONFLICT (id) DO NOTHING pour éviter les doublons.
        """
        if not enriched_events:
            return {"inserted": 0, "skipped": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        inserted = 0
        skipped = 0

        for event in enriched_events:
            cursor.execute(
                """INSERT INTO listening_events
                       (id, user_id, track_id, timestamp, duration_ms, device_type, geo_country, completed, event_source)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO NOTHING""",
                (
                    event["event_id"],
                    event["user_id"],
                    event["track_id"],
                    event["timestamp"],
                    event["duration_ms"],
                    event.get("device_type"),
                    event.get("geo_country"),
                    event.get("completed", False),
                    event.get("event_source", "p2p"),
                )
            )
            if cursor.rowcount > 0:
                inserted += 1
            else:
                skipped += 1

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Upsert : %d insérés, %d ignorés (doublons)", inserted, skipped)
        return {"inserted": inserted, "skipped": skipped}

    # ── Orchestration ─────────────────────────────────────────
    raw       = consume_from_redis()
    validated = validate_events(raw)
    enriched  = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)
