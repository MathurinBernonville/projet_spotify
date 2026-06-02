# Testing Guide for catalog_ingestion_pipeline DAG

This guide explains how to test locally the implementation of the `catalog_ingestion_pipeline` DAG.

## Prerequisites

- Docker and Docker Compose installed
- Python 3.8+ (for upload script)
- boto3 installed (optional: `pip install boto3`)

## Quick Start

### Option 1: Automated script (recommended)

```bash
# From project root
./start_and_test.sh
```

This script:

1. Stops existing containers
2. Starts docker-compose
3. Waits for all services to be ready (PostgreSQL, MinIO, Airflow)
4. Uploads test JSON files to MinIO
5. Displays access URLs

### Option 2: Manual

```bash
# 1. Start Docker Compose
docker-compose up -d

# 2. Wait 60-90 seconds

# 3. Upload data (if boto3 is installed)
python3 upload_to_minio.py

# 4. Access Airflow
open http://localhost:8080
```

## Available Services

Once started, services are accessible at:

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| MinIO Console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | spotify / spotify |
| Redis | localhost:6379 | - |

## DAG Verification

### 1. Verify DAG loads correctly

```bash
# In Airflow UI, go to "DAGs" section
# Search for "catalog_ingestion_pipeline"
# Verify there are no syntax errors
```

### 2. Trigger first execution

1. Click on `catalog_ingestion_pipeline` DAG
2. Click "Trigger DAG" button
3. Wait for execution to start (visible in "DAG Runs")

### 3. Check logs

```bash
# Option A: Via Airflow UI
# Click on DAG run -> Graph -> click each task -> "Logs"

# Option B: Via terminal
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-worker
```

### 4. Verify DLQ entries (optional)

```bash
# Connect to PostgreSQL
docker exec -it cours_hetic-postgres-1 psql -U spotify -d spotify -c \
  "SELECT id, error_type, error_message FROM dead_letter_events LIMIT 5;"
```

### 5. Verify inserted data

```bash
# Check artists inserted
docker exec -it cours_hetic-postgres-1 psql -U spotify -d spotify -c \
  "SELECT id, name, label, monthly_listeners FROM artists LIMIT 10;"

# Check tracks inserted
docker exec -it cours_hetic-postgres-1 psql -U spotify -d spotify -c \
  "SELECT id, title, duration_ms, genre FROM tracks LIMIT 10;"
```

## Test Idempotence

Idempotence means running the same DAG twice produces the same result.

```bash
# 1. Trigger DAG second time
# (From UI: Trigger DAG)

# 2. Check XCom values
# Values for "tracks_inserted", "artists_inserted" should be identical

# Or via SQL:
docker exec -it cours_hetic-postgres-1 psql -U spotify -d spotify -c \
  "SELECT COUNT(*) FROM tracks WHERE created_at > NOW() - INTERVAL '10 minutes';"
```

## Test Data

Test JSON files are in `test_data/`:

- `sunset_records.json`: 3 artists, 3 albums, 4 tracks
- `nightwave_music.json`: 3 artists, 3 albums, 4 tracks
- `urban_pulse.json`: 3 artists, 3 albums, 4 tracks

Total: 9 artists, 9 albums, 12 tracks

## Cleanup

```bash
# Stop containers
docker-compose down

# Clean volumes (warning: deletes data)
docker-compose down -v
```

## Validation Checklist

- [ ] Docker-compose starts without errors
- [ ] MinIO contains 3 JSONs (visible in MinIO Console)
- [ ] DAG `catalog_ingestion_pipeline` loads without error
- [ ] First DAG run displays in green
- [ ] Logs: all 5 tasks executed successfully
- [ ] XCom `tracks_inserted` > 0
- [ ] PostgreSQL contains 12 inserted tracks
- [ ] Second DAG run: same result (idempotence)
- [ ] No errors in DLQ (or predictable number)

## Troubleshooting

### MinIO connection refused

```bash
# Wait longer
sleep 30
docker-compose logs minio
```

### PostgreSQL access denied

```bash
# Check PostgreSQL started
docker-compose ps | grep postgres

# Check database
docker exec -it cours_hetic-postgres-1 psql -U airflow -l
```

### Airflow DAG import error

```bash
# Check Python syntax
python3 -m py_compile dags/catalog_ingestion_pipeline.py

# Check detailed errors
docker-compose logs airflow-init
docker-compose logs airflow-scheduler
```

### JSON files not uploaded

```bash
# Verify boto3 installed
pip install boto3

# Manual upload
python3 upload_to_minio.py
```

## Resources

- Apache Airflow Documentation: https://airflow.apache.org/
- MinIO S3 API: https://docs.min.io/
- PostgreSQL Documentation: https://www.postgresql.org/docs/

## Expected Results

Once DAG executes successfully:

```
catalog_ingestion_pipeline completed
DAGRun: 2026-06-02T00:00:00+00:00
Tracks inserted: 12
Artists inserted: 9
DLQ errors: 0
```

Done!
