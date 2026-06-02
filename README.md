# Spotify Data Platform — Groupe P

Plateforme de streaming musical distribuée construite dans le cadre du Master 1 Data & IA (HETIC 2026).

## Architecture
[200~Simulateur P2P (Python)
│
▼
Redis pub/sub + LIST buffer
│
▼
Airflow DAGs (batch)
├── catalog_ingestion_pipeline   → MinIO → PostgreSQL (artists, albums, tracks)
├── streaming_events_pipeline    → Redis → validation → MinIO Parquet + listening_events
├── aggregation_pipeline         → daily_streams + artist_stats
├── recommendation_pipeline      → Redis reco:{user_id} + recommendations
└── dlq_reprocessing_pipeline    → dead_letter_events retraitement
│
▼
Phase 2 : Kafka + Spark Structured Streaming
├── streaming_trends_job         → fenêtres 5 min
├── streaming_enrichment_job     → enrichissement métadonnées
└── fraud_detection_job          → détection bots
│
▼
Phase 3 : Inter-groupes
├── catalog_federation           → catalogue fédéré
└── Top 50 Global                → Redis top50:global
## Stack technique

| Couche | Technologie |
|---|---|
| Orchestration | Apache Airflow 2.9 |
| Messaging | Apache Kafka 3.6 (Phase 2) |
| Streaming | Spark 3.5 (Phase 2) |
| Base de données | PostgreSQL 15 |
| Cache | Redis 7 |
| Stockage objet | MinIO |
| Simulation | Python custom |
| Conteneurs | Docker Compose |

## Démarrage rapide

```bash
# Phase 1
cp .env.example .env
docker compose up -d
python -m src.data_generator.generate_catalog --artists 15
# Uploader dans MinIO puis trigger catalog_ingestion_pipeline dans Airflow

# Phase 2
docker compose -f docker-compose.yml -f docker-compose.kafka.yml up -d
```

## Interfaces

| Interface | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin |
| Kafka UI | http://localhost:8090 | — |

## Tests

```bash
set PYTHONPATH=.
pytest tests/ -v --tb=short
```

## Documentation

- [Data Model](docs/DATA_MODEL.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Runbook](docs/RUNBOOK.md)
