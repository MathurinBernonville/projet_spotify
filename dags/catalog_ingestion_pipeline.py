"""
DAG : catalog_ingestion_pipeline
"""

from datetime import datetime, timedelta
import json
import boto3
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists` (upsert)
- Table `albums` (upsert)
- Table `tracks` (upsert)

### Idempotence
Le pipeline est idempotent : relancer plusieurs fois le même DAGrun
produit le même résultat grâce aux upserts ON CONFLICT DO UPDATE.

### Gestion des erreurs
- Schéma invalide → événement en DLQ (`dead_letter_events`)
- MinIO indisponible → retry x3 avec backoff exponentiel
"""

DEFAULT_ARGS = {
    "owner":                     "spotify-team",
    "depends_on_past":           False,
    "start_date":                datetime(2025, 1, 1),
    "email_on_failure":          False,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]


with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        catalogs = []
        for filename in LABEL_FILES:
            try:
                obj = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                catalog = json.loads(obj["Body"].read())
                catalogs.append(catalog)
                logger.info("Fichier chargé : %s", filename)
            except Exception as e:
                logger.warning("Fichier manquant ou erreur : %s — %s", filename, e)
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        valid = {"artists": [], "albums": [], "tracks": []}
        errors_count = 0

        for catalog in raw_catalogs:
            for artist in catalog.get("artists", []):
                if all(k in artist for k in ["id", "name", "label"]):
                    valid["artists"].append(artist)
                else:
                    errors_count += 1
                    cursor.execute(
                        """INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message)
                           VALUES (%s, %s::jsonb, %s, %s)""",
                        ("minio_catalog", json.dumps(artist), "schema_validation", "Champs obligatoires manquants dans artist"),
                    )

            for album in catalog.get("albums", []):
                if all(k in album for k in ["id", "artist_id", "title"]):
                    valid["albums"].append(album)
                else:
                    errors_count += 1
                    cursor.execute(
                        """INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message)
                           VALUES (%s, %s::jsonb, %s, %s)""",
                        ("minio_catalog", json.dumps(album), "schema_validation", "Champs obligatoires manquants dans album"),
                    )

cat > dags/catalog_ingestion_pipeline.py << 'EOF'
"""
DAG : catalog_ingestion_pipeline
"""

from datetime import datetime, timedelta
import json
import boto3
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
(SunSet Records, NightWave Music, Urban Pulse) stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists` (upsert)
- Table `albums` (upsert)
- Table `tracks` (upsert)

### Idempotence
Le pipeline est idempotent : relancer plusieurs fois le même DAGrun
produit le même résultat grâce aux upserts ON CONFLICT DO UPDATE.

### Gestion des erreurs
- Schéma invalide → événement en DLQ (`dead_letter_events`)
- MinIO indisponible → retry x3 avec backoff exponentiel
"""

DEFAULT_ARGS = {
    "owner":                     "spotify-team",
    "depends_on_past":           False,
    "start_date":                datetime(2025, 1, 1),
    "email_on_failure":          False,
    "email_on_retry":            False,
    "retries":                   3,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_ENDPOINT   = "http://minio:9000"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]


with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
        )
        catalogs = []
        for filename in LABEL_FILES:
            try:
                obj = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                catalog = json.loads(obj["Body"].read())
                catalogs.append(catalog)
                logger.info("Fichier chargé : %s", filename)
            except Exception as e:
                logger.warning("Fichier manquant ou erreur : %s — %s", filename, e)
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        valid = {"artists": [], "albums": [], "tracks": []}
        errors_count = 0

        for catalog in raw_catalogs:
            for artist in catalog.get("artists", []):
                if all(k in artist for k in ["id", "name", "label"]):
                    valid["artists"].append(artist)
                else:
                    errors_count += 1
                    cursor.execute(
                        """INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message)
                           VALUES (%s, %s::jsonb, %s, %s)""",
                        ("minio_catalog", json.dumps(artist), "schema_validation", "Champs obligatoires manquants dans artist"),
                    )

            for album in catalog.get("albums", []):
                if all(k in album for k in ["id", "artist_id", "title"]):
                    valid["albums"].append(album)
                else:
                    errors_count += 1
                    cursor.execute(
                        """INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message)
                           VALUES (%s, %s::jsonb, %s, %s)""",
                        ("minio_catalog", json.dumps(album), "schema_validation", "Champs obligatoires manquants dans album"),
                    )

            for track in catalog.get("tracks", []):
                if all(k in track for k in ["id", "artist_id", "title", "duration_ms"]):
                    valid["tracks"].append(track)
                else:
                    errors_count += 1
                    cursor.execute(
                        """INSERT INTO dead_letter_events (original_topic, payload, error_type, error_message)
                           VALUES (%s, %s::jsonb, %s, %s)""",
                        ("minio_catalog", json.dumps(track), "schema_validation", "Champs obligatoires manquants dans track"),
                    )

        conn.commit()
        cursor.close()
        conn.close()
        logger.info("Validation : %d erreurs DLQ", errors_count)
        return {"valid": valid, "errors_count": errors_count}

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        data = validated["valid"]

        artists = []
        seen_artists = set()
        for a in data["artists"]:
            name = a["name"].strip().title()
            label = a.get("label", "").strip()
            key = (name, label)
            if key not in seen_artists:
                seen_artists.add(key)
                artists.append({
                    **a,
                    "name": name,
                    "label": label,
                    "genres": a.get("genres", []),
                    "monthly_listeners": a.get("monthly_listeners", 0),
                })

        tracks = []
        for t in data["tracks"]:
            duration = t.get("duration_ms", 0)
            if 0 < duration < 3_600_000:
                tracks.append({**t, "title": t["title"].strip()})

        return {
            "artists": artists,
            "albums": data["albums"],
            "tracks": tracks,
        }

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        artists_inserted = 0
        for a in transformed["artists"]:
            cursor.execute(
                """INSERT INTO artists (id, name, label, genres, monthly_listeners)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (name, label) DO UPDATE SET
                       monthly_listeners = EXCLUDED.monthly_listeners,
                       updated_at = NOW()""",
                (a["id"], a["name"], a["label"], a.get("genres", []), a.get("monthly_listeners", 0)),
            )
            artists_inserted += 1

        albums_inserted = 0
        for al in transformed["albums"]:
            cursor.execute(
                """INSERT INTO albums (id, artist_id, title, release_year, total_tracks)
                   VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO UPDATE SET
                       title = EXCLUDED.title""",
                (al["id"], al["artist_id"], al["title"], al.get("release_year"), al.get("total_tracks")),
            )
            albums_inserted += 1

        tracks_inserted = 0
        for t in transformed["tracks"]:
            cursor.execute(
                """INSERT INTO tracks (id, artist_id, album_id, title, duration_ms, genre, bpm, explicit)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO UPDATE SET
                       updated_at = NOW()""",
                (
                    t["id"], t["artist_id"], t.get("album_id"),
                    t["title"], t["duration_ms"],
                    t.get("genre"), t.get("bpm"), t.get("explicit", False),
                ),
            )
            tracks_inserted += 1

        conn.commit()
        cursor.close()
        conn.close()

        stats = {
            "artists_inserted": artists_inserted,
            "albums_inserted": albums_inserted,
            "tracks_inserted": tracks_inserted,
        }
        logger.info("Stats chargement : %s", stats)
        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun : {dag_run.run_id}
        Tracks insérées  : {stats.get('tracks_inserted', 0)}
        Artists insérés  : {stats.get('artists_inserted', 0)}
        Erreurs DLQ      : {stats.get('errors_count', 0)}
        """)

    raw         = extract_from_minio()
    validated   = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats       = load_to_postgres(transformed)
    notify_success(stats)
