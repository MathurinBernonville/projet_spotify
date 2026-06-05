Spotify Data Platform — Groupe P
Plateforme de streaming musical distribuée construite dans le cadre du Master 1 Data & IA (HETIC 2026).


Architecture
Simulateur P2P (Python)
│
├── Redis pub/sub + LIST buffer ──► Airflow DAGs (batch)
│                                   ├── catalog_ingestion_pipeline   → MinIO → PostgreSQL
│                                   ├── streaming_events_pipeline    → Redis → Parquet + listening_events
│                                   ├── aggregation_pipeline         → daily_streams + artist_stats
│                                   ├── recommendation_pipeline      → Redis reco:{user_id}
│                                   └── dlq_reprocessing_pipeline    → dead_letter_events
│
└── Kafka (3 brokers KRaft) ──────► Spark Structured Streaming
                                    ├── streaming_trends_job         → top tracks 5min → PostgreSQL + Redis
                                    │   ├── watermark 10 min
                                    │   ├── late events → topic late_listening_events
                                    │   └── checkpoints MinIO (exactly-once)
                                    ├── streaming_enrichment_job     → enrichissement métadonnées
                                    └── fraud_detection_job          → détection bots
Stack technique
CoucheTechnologieVersionOrchestrationApache Airflow2.9MessagingApache Kafka KRaft3.6StreamingSpark Structured Streaming3.5.1Base de donnéesPostgreSQL15CacheRedis7Stockage objetMinIOlatestSimulationPython custom—ConteneursDocker Compose—
Démarrage rapide
bash# Cloner et configurer
git clone https://github.com/MathurinBernonville/projet_spotify.git
cd projet_spotify
cp .env.example .env

# Phase 1 — stack batch
docker compose up -d
sleep 60

# Générer les données
python -m src.data_generator.generate_catalog --artists 15

# Uploader dans MinIO et trigger catalog_ingestion_pipeline dans Airflow

# Phase 2 — simulateur Kafka
python -m src.p2p_simulator.simulator --kafka --peers 10 --rate 5

# Phase 2 — job Spark (dans un autre terminal)
docker cp spark_jobs/streaming_trends_job.py cours_hetic-spark-master-1:/tmp/streaming_trends_job.py
docker exec cours_hetic-spark-master-1 //opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1,org.apache.hadoop:hadoop-aws:3.3.4 \
  //tmp/streaming_trends_job.py
Interfaces
InterfaceURLCredentialsAirflowhttp://localhost:8080admin / adminMinIOhttp://localhost:9001minioadmin / minioadminKafka UIhttp://localhost:8090—
Tests
bashexport PYTHONPATH=$(pwd)
pytest tests/unit/ -v --tb=short
# Résultat attendu : 18 passed
Vérifications exactly-once
bash# 0 doublon après redémarrage Spark
docker exec -it cours_hetic-postgres-1 psql -U spotify spotify \
  -c "SELECT COUNT(*) - COUNT(DISTINCT id) AS doublons FROM listening_events;"

# Late events dans Kafka
# Kafka UI → topic late_listening_events → Messages > 0

# Checkpoints MinIO
# http://localhost:9001 → bucket spotify-checkpoints
Validation Phase 2
bash# Top tracks temps réel
docker exec -it cours_hetic-postgres-1 psql -U spotify spotify \
  -c "SELECT * FROM realtime_top_tracks ORDER BY stream_count DESC LIMIT 5;"

# Genres dans Redis
docker exec -it cours_hetic-redis-1 redis-cli get genre_listeners:live

# Recommandations
docker exec -it cours_hetic-redis-1 redis-cli keys "reco:*" | wc -l
Documentation

Data Model
Architecture
Runbook

Groupe P
Mathurin Bernonville — Phase 1 complète + Phase 2 (issues #11 à #16)
