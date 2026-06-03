"""
DAG : recommendation_pipeline
================================
Génère les recommandations personnalisées via collaborative filtering
et les stocke dans Redis + PostgreSQL.

Dépend de aggregation_pipeline via ExternalTaskSensor.
"""

from datetime import datetime, timedelta
import json
import logging

import numpy as np
import pandas as pd
import redis as redis_lib
from sklearn.metrics.pairwise import cosine_similarity

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## recommendation_pipeline

### Rôle
Génère un top-10 de recommandations par utilisateur actif
via collaborative filtering (similarité cosinus entre profils d'écoute).

### Dépendances
Attend la fin de `aggregation_pipeline` via ExternalTaskSensor.

### Destinations
- Redis : clé `reco:{user_id}` → liste de track_ids (TTL 24h)
- PostgreSQL : table `recommendations`

### Algorithme
Collaborative filtering simplifié :
1. Construire la matrice user × track (écoutes des 7 derniers jours)
2. Calculer la similarité cosinus entre utilisateurs
3. Pour chaque user, recommander les tracks aimés par ses voisins
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=10),
    "execution_timeout": timedelta(minutes=45),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_URL        = "redis://redis:6379/1"
RECO_TTL_SECONDS = 86400   # 24 heures
TOP_N_RECO       = 10
LOOKBACK_DAYS    = 7


with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Collaborative filtering → recommandations Redis + PostgreSQL",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommendation", "ml"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """Construit la matrice user x track des écoutes des 7 derniers jours."""
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT user_id::text, track_id::text, COUNT(*) AS play_count
            FROM listening_events
            WHERE timestamp >= NOW() - INTERVAL '7 days'
              AND completed = TRUE
            GROUP BY user_id, track_id
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if not rows:
            logger.warning("Aucune donnée d'écoute pour construire la matrice.")
            return {"matrix": {}, "active_users": [], "all_tracks": []}

        # Construire dict {user_id: {track_id: play_count}}
        matrix = {}
        for user_id, track_id, play_count in rows:
            if user_id not in matrix:
                matrix[user_id] = {}
            matrix[user_id][track_id] = play_count

        # Garder uniquement les users avec >= 3 écoutes distinctes
        active_users = [u for u, tracks in matrix.items() if len(tracks) >= 3]
        all_tracks = list({track for tracks in matrix.values() for track in tracks})

        logger.info("Matrice construite : %d users actifs, %d tracks", len(active_users), len(all_tracks))
        return {"matrix": matrix, "active_users": active_users, "all_tracks": all_tracks}

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict, **context) -> dict:
        """Calcule les recommandations par similarité cosinus."""
        matrix = matrix_data.get("matrix", {})
        active_users = matrix_data.get("active_users", [])
        all_tracks = matrix_data.get("all_tracks", [])

        if not active_users or not all_tracks:
            logger.warning("Pas assez de données pour calculer les recommandations.")
            return {}

        # Construire DataFrame user x track
        track_index = {t: i for i, t in enumerate(all_tracks)}
        user_index = {u: i for i, u in enumerate(active_users)}

        mat = np.zeros((len(active_users), len(all_tracks)))
        for user_id in active_users:
            for track_id, count in matrix[user_id].items():
                if track_id in track_index:
                    mat[user_index[user_id], track_index[track_id]] = count

        # Calcul similarité cosinus
        sim_matrix = cosine_similarity(mat)

        recommendations = {}
        for i, user_id in enumerate(active_users):
            # Top 5 voisins les plus similaires (excluant soi-même)
            sim_scores = list(enumerate(sim_matrix[i]))
            sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
            top_neighbors = [active_users[j] for j, score in sim_scores[1:6] if score > 0]

            # Tracks déjà écoutés par cet utilisateur
            already_listened = set(matrix[user_id].keys())

            # Tracks recommandés par les voisins
            candidate_tracks = {}
            for neighbor in top_neighbors:
                for track_id, count in matrix[neighbor].items():
                    if track_id not in already_listened:
                        candidate_tracks[track_id] = candidate_tracks.get(track_id, 0) + count

            # Top N recommandations
            top_tracks = sorted(candidate_tracks.items(), key=lambda x: x[1], reverse=True)[:TOP_N_RECO]
            if top_tracks:
                recommendations[user_id] = [t for t, _ in top_tracks]

        logger.info("Recommandations calculées pour %d utilisateurs", len(recommendations))
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """Stocke les recommandations dans Redis et PostgreSQL."""
        if not recommendations:
            logger.warning("Aucune recommandation à stocker.")
            return {"users_with_recos": 0, "total_recommendations": 0}

        # Redis
        r = redis_lib.from_url(REDIS_URL, decode_responses=True)
        for user_id, track_ids in recommendations.items():
            r.setex(f"reco:{user_id}", RECO_TTL_SECONDS, json.dumps(track_ids))

        # PostgreSQL
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        total = 0
        for user_id, track_ids in recommendations.items():
            for i, track_id in enumerate(track_ids):
                score = 1.0 - (i * 0.1)  # score décroissant selon le rang
                cursor.execute("""
                    INSERT INTO recommendations (user_id, track_id, score, generated_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (user_id, track_id) DO UPDATE SET
                        score        = EXCLUDED.score,
                        generated_at = NOW()
                """, (user_id, track_id, score))
                total += 1

        conn.commit()
        cursor.close()
        conn.close()

        result = {"users_with_recos": len(recommendations), "total_recommendations": total}
        logger.info("Recommandations stockées : %s", result)
        return result

    # ── Orchestration ─────────────────────────────────────────
    matrix          = build_user_track_matrix()
    recommendations = compute_recommendations(matrix)

    wait_for_aggregation >> matrix
    store_recommendations(recommendations)