# Architecture — Spotify Platform

## Choix ETL vs ELT par pipeline

| Pipeline | Choix | Justification |
|---|---|---|
| catalog_ingestion | ETL | Transformation JSON → relationnel avant chargement PostgreSQL |
| streaming_events | ETL | Agrégation Spark avant écriture (fenêtres, watermark) |
| aggregation | ELT | Données brutes chargées dans PostgreSQL, agrégées en SQL sur place |
| recommendation | ELT | Calcul de score directement sur les données PostgreSQL existantes |
| dlq_reprocessing | ETL | Retraitement et nettoyage avant réinsertion |

## Stack technique

| Couche | Technologie | Rôle |
|---|---|---|
| Orchestration | Airflow 2.9 | DAGs, scheduling, retry |
| Messaging | Kafka 3.6 | Topics, partitions (Phase 2) |
| Streaming | Spark 3.5 | Structured Streaming, exactly-once |
| Base de données | PostgreSQL 15 | Catalogue, événements, agrégats |
| Cache | Redis 7 | Recommandations, Top tracks live |
| Stockage objet | MinIO | Parquet, checkpoints Spark |
