"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor.

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `execution_date` (le jour courant).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """Calcule le top 50 des tracks pour la date d'exécution."""
        from datetime import date
        execution_date = date.today()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                track_id::text,
                COUNT(*) AS total_streams,
                COUNT(DISTINCT user_id) AS unique_listeners,
                SUM(duration_ms) AS total_duration_ms,
                ARRAY_AGG(DISTINCT geo_country) AS countries
            FROM listening_events
            WHERE DATE(timestamp) = %s AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50
        """, (execution_date,))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        result = [
            {
                "track_id": row[0],
                "date": str(execution_date),
                "total_streams": row[1],
                "unique_listeners": row[2],
                "total_duration_ms": row[3],
                "countries": row[4] or [],
            }
            for row in rows
        ]
        logger.info("Top tracks calculés : %d tracks pour le %s", len(result), execution_date)
        return result

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """Calcule les statistiques par artiste pour la date d'exécution."""
        execution_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                t.artist_id::text,
                DATE(le.timestamp) AS date,
                COUNT(*) AS total_streams,
                COUNT(DISTINCT le.user_id) AS unique_listeners,
                MODE() WITHIN GROUP (ORDER BY le.track_id::text) AS top_track_id
            FROM listening_events le
            JOIN tracks t ON le.track_id = t.id
            WHERE DATE(le.timestamp) = %s
            GROUP BY t.artist_id, DATE(le.timestamp)
        """, (execution_date,))

        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        result = [
            {
                "artist_id": row[0],
                "date": str(row[1]),
                "total_streams": row[2],
                "unique_listeners": row[3],
                "top_track_id": row[4],
            }
            for row in rows
        ]
        logger.info("Artist stats calculées : %d artistes pour le %s", len(result), execution_date)
        return result

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """Calcule les métriques du réseau P2P pour la date d'exécution."""
        execution_date = context["data_interval_start"].date()
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN event_source = 'cache' THEN 1 ELSE 0 END) AS cache_hits,
                SUM(CASE WHEN event_source = 'p2p' THEN 1 ELSE 0 END) AS p2p_count,
                COUNT(DISTINCT user_id) AS unique_listeners,
                AVG(duration_ms) AS avg_duration_ms
            FROM listening_events
            WHERE DATE(timestamp) = %s
        """, (execution_date,))

        row = cursor.fetchone()
        cursor.close()
        conn.close()

        total = row[0] or 0
        metrics = {
            "date": str(execution_date),
            "total_events": total,
            "cache_hit_rate": round(row[1] / total, 4) if total > 0 else 0,
            "p2p_ratio": round(row[2] / total, 4) if total > 0 else 0,
            "unique_listeners": row[3] or 0,
            "avg_duration_ms": round(float(row[4]), 2) if row[4] else 0,
        }
        logger.info("Métriques P2P : %s", metrics)
        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        """Écrit les agrégats dans PostgreSQL de façon idempotente."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Upsert daily_streams
        for track in top_tracks:
            cursor.execute("""
                INSERT INTO daily_streams
                    (track_id, date, total_streams, unique_listeners, total_duration_ms, countries)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (track_id, date) DO UPDATE SET
                    total_streams     = EXCLUDED.total_streams,
                    unique_listeners  = EXCLUDED.unique_listeners,
                    total_duration_ms = EXCLUDED.total_duration_ms,
                    countries         = EXCLUDED.countries,
                    updated_at        = NOW()
            """, (
                track["track_id"],
                track["date"],
                track["total_streams"],
                track["unique_listeners"],
                track["total_duration_ms"],
                track["countries"],
            ))

        # Upsert artist_stats
        for stat in artist_stats:
            cursor.execute("""
                INSERT INTO artist_stats
                    (artist_id, date, total_streams, unique_listeners, top_track_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (artist_id, date) DO UPDATE SET
                    total_streams    = EXCLUDED.total_streams,
                    unique_listeners = EXCLUDED.unique_listeners,
                    top_track_id     = EXCLUDED.top_track_id,
                    updated_at       = NOW()
            """, (
                stat["artist_id"],
                stat["date"],
                stat["total_streams"],
                stat["unique_listeners"],
                stat["top_track_id"],
            ))

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(
            "Agrégats mis à jour : %d daily_streams, %d artist_stats",
            len(top_tracks), len(artist_stats)
        )

    # ── Orchestration ─────────────────────────────────────────
    top_tracks_result   = compute_top_tracks()
    artist_stats_result = compute_artist_stats()
    p2p_metrics_result  = compute_p2p_metrics()

    wait_for_events >> [top_tracks_result, artist_stats_result, p2p_metrics_result]
    update_aggregates(top_tracks_result, artist_stats_result, p2p_metrics_result)