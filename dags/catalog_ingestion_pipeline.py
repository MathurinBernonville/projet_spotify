"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
(stockés dans MinIO) et les charge dans PostgreSQL.

Planification : quotidienne à 02:00 UTC
Catchup       : activé (permet le backfill historique)

Architecture :
    MinIO (labels/*.json)
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()        ← normalisation, dédoublonnage
        → load_to_postgres()         ← upsert avec ON CONFLICT
        → notify_success()

TODO :
    [ ] Implémenter extract_from_minio() — lire les JSONs depuis MinIO
    [ ] Implémenter validate_schema() — vérifier les champs obligatoires
    [ ] Implémenter transform_catalog() — normaliser les noms d'artistes, déduplication
    [ ] Implémenter load_to_postgres() — upsert avec gestion des conflits
    [ ] Configurer retry_delay et retries sur les tâches réseau
    [ ] Ajouter un on_failure_callback pour alerting
    [ ] Activer le doc_md sur ce DAG (voir variable DAG_DOC ci-dessous)
"""

from datetime import datetime, timedelta
import json
import boto3
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable

# ─────────────────────────────────────────────────────────────
# DOCUMENTATION DU DAG (obligatoire pour la note)
# ─────────────────────────────────────────────────────────────

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

### Monitoring
- XCom `tracks_inserted` : nombre de tracks insérées/mises à jour
- XCom `errors_count` : nombre d'entrées envoyées en DLQ
"""

# ─────────────────────────────────────────────────────────────
# CONFIGURATION PAR DÉFAUT
# ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":                 "spotify-team",
    "depends_on_past":       False,
    "start_date":            datetime(2025, 1, 1),
    "email_on_failure":      False,
    "email_on_retry":        False,
    "retries":               3,
    "retry_delay":           timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":     timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"
MINIO_CONN_ID    = "spotify_minio"
MINIO_BUCKET     = "labels-raw"
LABEL_FILES      = ["sunset_records.json", "nightwave_music.json", "urban_pulse.json"]


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

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
    def extract_from_minio(**context) -> list[dict]:
        """
        Télécharge les fichiers JSON des labels depuis MinIO.

        TODO :
            1. Se connecter à MinIO via AwsBaseHook ou boto3
               (endpoint_url = http://minio:9000)
            2. Pour chaque fichier dans LABEL_FILES, télécharger et parser le JSON
            3. Retourner une liste de catalogues : [catalog_label_a, catalog_label_b, ...]
            4. Si un fichier est manquant : logger un warning et continuer
               (pas de crash — on traite ce qu'on a)

        Returns:
            list[dict] : catalogues bruts des labels
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # Initialiser le client S3/MinIO
        s3_client = boto3.client(
            's3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name='us-east-1'
        )
        
        catalogs = []
        
        for file_name in LABEL_FILES:
            try:
                # Télécharger le fichier depuis MinIO
                response = s3_client.get_object(Bucket=MINIO_BUCKET, Key=file_name)
                file_content = response['Body'].read().decode('utf-8')
                
                # Parser le JSON
                catalog = json.loads(file_content)
                catalogs.append(catalog)
                logger.info(f"✓ Extrait {file_name} depuis MinIO")
                
            except ClientError as e:
                logger.warning(f"⚠ Fichier {file_name} manquant dans MinIO: {str(e)}")
                continue
            except json.JSONDecodeError as e:
                logger.warning(f"⚠ Erreur parsing JSON pour {file_name}: {str(e)}")
                continue
        
        logger.info(f"Extraction terminée : {len(catalogs)} catalogues chargés")
        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """
        Valide le schéma de chaque catalogue et isole les entrées invalides.

        Champs obligatoires pour un artiste  : id, name, label
        Champs obligatoires pour un album    : id, artist_id, title
        Champs obligatoires pour un track    : id, artist_id, title, duration_ms

        TODO :
            1. Parcourir artists, albums, tracks de chaque catalogue
            2. Pour chaque entrée, vérifier la présence des champs obligatoires
            3. Les entrées invalides → insérer dans dead_letter_events avec error_type="schema_validation"
            4. Retourner {"valid": {...}, "errors_count": N}

        Hint : utiliser PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        """
        import logging
        logger = logging.getLogger(__name__)
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        valid_data = {
            'artists': [],
            'albums': [],
            'tracks': []
        }
        errors_count = 0
        
        # Schémas de validation
        required_fields = {
            'artists': ['id', 'name', 'label'],
            'albums': ['id', 'artist_id', 'title'],
            'tracks': ['id', 'artist_id', 'title', 'duration_ms']
        }
        
        for catalog in raw_catalogs:
            # Valider les artistes
            for entity_type in ['artists', 'albums', 'tracks']:
                entities = catalog.get(entity_type, [])
                
                for entity in entities:
                    # Vérifier les champs obligatoires
                    missing_fields = [f for f in required_fields[entity_type] if f not in entity]
                    
                    if missing_fields:
                        # Envoyer en DLQ
                        error_payload = {
                            'entity_type': entity_type,
                            'data': entity,
                            'missing_fields': missing_fields
                        }
                        pg_hook.run("""
                            INSERT INTO dead_letter_events 
                            (original_topic, payload, error_type, error_message, status)
                            VALUES (%s, %s, %s, %s, %s)
                        """, parameters=(
                            'catalog_ingestion',
                            json.dumps(error_payload),
                            'schema_validation',
                            f"Champs manquants pour {entity_type}: {', '.join(missing_fields)}",
                            'pending'
                        ))
                        errors_count += 1
                        logger.warning(f"⚠ Entrée invalide en {entity_type}: {missing_fields}")
                    else:
                        # Ajouter aux données valides
                        valid_data[entity_type].append(entity)
        
        logger.info(f"Validation terminée : {len(valid_data['artists'])} artistes, "
                   f"{len(valid_data['albums'])} albums, "
                   f"{len(valid_data['tracks'])} tracks, "
                   f"{errors_count} erreurs en DLQ")
        
        return {
            'valid': valid_data,
            'errors_count': errors_count
        }

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Transforme et normalise les données du catalogue.

        TODO :
            1. Normaliser les noms d'artistes (strip, title case, suppression doublons)
            2. Valider les durées de tracks (duration_ms > 0 et < 3_600_000)
            3. Normaliser les genres (correspondance avec la table genres)
            4. Construire les listes d'upsert : artists[], albums[], tracks[]

        Returns:
            dict avec keys "artists", "albums", "tracks"
        """
        import logging
        logger = logging.getLogger(__name__)
        
        valid_data = validated['valid']
        
        transformed = {
            'artists': [],
            'albums': [],
            'tracks': []
        }
        
        # Normaliser les artistes
        seen_artists = set()  # Pour déduplication
        for artist in valid_data['artists']:
            # Normaliser le nom
            normalized_name = artist.get('name', '').strip().title()
            
            # Clé de déduplication
            artist_key = (normalized_name, artist.get('label', ''))
            
            if artist_key not in seen_artists:
                normalized_artist = {
                    'id': artist.get('id'),
                    'name': normalized_name,
                    'country': artist.get('country'),
                    'label': artist.get('label'),
                    'genres': artist.get('genres', []),
                    'monthly_listeners': artist.get('monthly_listeners', 0)
                }
                transformed['artists'].append(normalized_artist)
                seen_artists.add(artist_key)
                logger.info(f"✓ Artiste normalisé: {normalized_name}")
        
        # Albums (peu de normalisation nécessaire)
        for album in valid_data['albums']:
            normalized_album = {
                'id': album.get('id'),
                'artist_id': album.get('artist_id'),
                'title': album.get('title', '').strip(),
                'release_year': album.get('release_year'),
                'total_tracks': album.get('total_tracks')
            }
            transformed['albums'].append(normalized_album)
        
        # Normaliser les tracks
        for track in valid_data['tracks']:
            duration_ms = track.get('duration_ms', 0)
            
            # Valider la durée
            if duration_ms <= 0 or duration_ms > 3_600_000:  # > 1 heure
                logger.warning(f"⚠ Durée invalide pour track {track.get('id')}: {duration_ms}ms")
                continue
            
            normalized_track = {
                'id': track.get('id'),
                'album_id': track.get('album_id'),
                'artist_id': track.get('artist_id'),
                'title': track.get('title', '').strip(),
                'duration_ms': duration_ms,
                'genre': track.get('genre', '').strip(),
                'bpm': track.get('bpm'),
                'explicit': track.get('explicit', False),
                'audio_file_path': track.get('audio_file_path')
            }
            transformed['tracks'].append(normalized_track)
            logger.info(f"✓ Track normalisé: {normalized_track['title']}")
        
        logger.info(f"Transformation terminée : "
                   f"{len(transformed['artists'])} artistes, "
                   f"{len(transformed['albums'])} albums, "
                   f"{len(transformed['tracks'])} tracks")
        
        return transformed

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Charge les données dans PostgreSQL avec upsert idempotent.

        TODO :
            1. Utiliser PostgresHook pour obtenir une connexion
            2. Artists : INSERT ... ON CONFLICT (name, label) DO UPDATE SET ...
            3. Albums  : INSERT ... ON CONFLICT (id) DO UPDATE SET ...
            4. Tracks  : INSERT ... ON CONFLICT (id) DO UPDATE SET updated_at=NOW()
            5. Commit et retourner les stats {tracks_inserted, artists_inserted, ...}
            6. Pousser stats dans XCom pour le monitoring

        Hint : utiliser executemany() avec des listes de tuples pour les performances.
        """
        import logging
        logger = logging.getLogger(__name__)
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        stats = {
            'artists_inserted': 0,
            'albums_inserted': 0,
            'tracks_inserted': 0
        }
        
        # ─── UPSERT ARTISTS ───────────────────────────────────────
        if transformed['artists']:
            artists_tuples = [
                (
                    artist['id'],
                    artist['name'],
                    artist.get('country'),
                    artist.get('label'),
                    artist.get('genres', []),
                    artist.get('monthly_listeners', 0)
                )
                for artist in transformed['artists']
            ]
            
            insert_artists_sql = """
            INSERT INTO artists (id, name, country, label, genres, monthly_listeners)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (name, label) DO UPDATE SET
                country = EXCLUDED.country,
                genres = EXCLUDED.genres,
                monthly_listeners = EXCLUDED.monthly_listeners,
                updated_at = NOW()
            """
            pg_hook.insert_rows(
                table='artists',
                rows=artists_tuples,
                target_fields=['id', 'name', 'country', 'label', 'genres', 'monthly_listeners'],
                replace=False
            )
            # Utiliser executemany directement pour le ON CONFLICT
            conn = pg_hook.get_conn()
            cur = conn.cursor()
            cur.executemany(insert_artists_sql, artists_tuples)
            conn.commit()
            stats['artists_inserted'] = len(artists_tuples)
            logger.info(f"✓ {len(artists_tuples)} artistes upsertés")
        
        # ─── UPSERT ALBUMS ────────────────────────────────────────
        if transformed['albums']:
            albums_tuples = [
                (
                    album['id'],
                    album['artist_id'],
                    album['title'],
                    album.get('release_year'),
                    album.get('total_tracks')
                )
                for album in transformed['albums']
            ]
            
            insert_albums_sql = """
            INSERT INTO albums (id, artist_id, title, release_year, total_tracks)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                title = EXCLUDED.title,
                release_year = EXCLUDED.release_year,
                total_tracks = EXCLUDED.total_tracks
            """
            conn = pg_hook.get_conn()
            cur = conn.cursor()
            cur.executemany(insert_albums_sql, albums_tuples)
            conn.commit()
            stats['albums_inserted'] = len(albums_tuples)
            logger.info(f"✓ {len(albums_tuples)} albums upsertés")
        
        # ─── UPSERT TRACKS ────────────────────────────────────────
        if transformed['tracks']:
            tracks_tuples = [
                (
                    track['id'],
                    track.get('album_id'),
                    track['artist_id'],
                    track['title'],
                    track['duration_ms'],
                    track.get('genre'),
                    track.get('bpm'),
                    track.get('explicit', False),
                    track.get('audio_file_path')
                )
                for track in transformed['tracks']
            ]
            
            insert_tracks_sql = """
            INSERT INTO tracks 
            (id, album_id, artist_id, title, duration_ms, genre, bpm, explicit, audio_file_path)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                album_id = EXCLUDED.album_id,
                title = EXCLUDED.title,
                duration_ms = EXCLUDED.duration_ms,
                genre = EXCLUDED.genre,
                bpm = EXCLUDED.bpm,
                explicit = EXCLUDED.explicit,
                audio_file_path = EXCLUDED.audio_file_path,
                updated_at = NOW()
            """
            conn = pg_hook.get_conn()
            cur = conn.cursor()
            cur.executemany(insert_tracks_sql, tracks_tuples)
            conn.commit()
            stats['tracks_inserted'] = len(tracks_tuples)
            logger.info(f"✓ {len(tracks_tuples)} tracks upsertés")
        
        # Pousser les stats dans XCom pour le monitoring
        context['ti'].xcom_push(key='tracks_inserted', value=stats['tracks_inserted'])
        context['ti'].xcom_push(key='artists_inserted', value=stats['artists_inserted'])
        context['ti'].xcom_push(key='albums_inserted', value=stats['albums_inserted'])
        
        logger.info(f"Chargement terminé : {stats}")
        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        """
        Log de succès avec statistiques d'ingestion.
        Optionnel : envoyer une notification (webhook Slack simulé).
        """
        dag_run = context["dag_run"]
        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun : {dag_run.run_id}
        Tracks insérées  : {stats.get('tracks_inserted', 0)}
        Artists insérés  : {stats.get('artists_inserted', 0)}
        Erreurs DLQ      : {stats.get('errors_count', 0)}
        """)

    # ── Orchestration des tâches ──────────────────────────────
    raw       = extract_from_minio()
    validated = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats     = load_to_postgres(transformed)
    notify_success(stats)
