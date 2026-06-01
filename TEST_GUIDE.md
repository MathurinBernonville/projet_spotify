# 🧪 Guide de test du DAG `catalog_ingestion_pipeline`

Ce guide explique comment tester localement l'implémentation du DAG `catalog_ingestion_pipeline`.

## 📋 Prérequis

- ✅ Docker et Docker Compose installés
- ✅ Python 3.8+ (pour le script d'upload)
- ✅ boto3 installé (optionnel : `pip install boto3`)

## 🚀 Démarrage rapide

### Option 1 : Script automatisé (recommandé)

```bash
# Depuis la racine du projet
./start_and_test.sh
```

Ce script :

1. ✅ Arrête les conteneurs existants
2. ✅ Démarre docker-compose
3. ✅ Attend que tous les services soient prêts (PostgreSQL, MinIO, Airflow)
4. ✅ Upload les fichiers JSON de test dans MinIO
5. ✅ Affiche les URLs d'accès

### Option 2 : Manuel

```bash
# 1. Démarrer Docker Compose
docker-compose up -d

# 2. Attendre 60-90 secondes

# 3. Upload les données (si boto3 est installé)
python3 upload_to_minio.py

# 4. Accéder à Airflow
open http://localhost:8080
```

## 📊 Services disponibles

Une fois démarrés, les services sont accessibles sur :

| Service           | URL                   | Credentials             |
| ----------------- | --------------------- | ----------------------- |
| **Airflow UI**    | http://localhost:8080 | admin / admin           |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |
| **PostgreSQL**    | localhost:5432        | spotify / spotify       |
| **Redis**         | localhost:6379        | -                       |

## 🔍 Vérification du DAG

### 1. Vérifier que le DAG se charge

```bash
# Dans l'UI Airflow, aller dans "DAGs"
# Chercher "catalog_ingestion_pipeline"
# Vérifier qu'il n'y a pas d'erreur de syntaxe
```

### 2. Lancer une première exécution

1. Cliquez sur le DAG `catalog_ingestion_pipeline`
2. Cliquez sur le bouton **"Trigger DAG"**
3. Attendez que l'exécution commence (visible dans le "DAG Runs")

### 3. Vérifier les logs

```bash
# Option A : Via la UI Airflow
# Cliquez sur le DAG run → Graph → cliquez sur chaque task → "Logs"

# Option B : Via le terminal
docker-compose logs -f airflow-scheduler
docker-compose logs -f airflow-worker
```

### 4. Vérifier le résultat en DLQ (optionnel)

```bash
# Connectez-vous à PostgreSQL
docker exec -it cours_hetic-postgres-1 psql -U spotify -d spotify -c \
  "SELECT id, error_type, error_message FROM dead_letter_events LIMIT 5;"
```

### 5. Vérifier les données insérées

```bash
# Voir les artistes insérés
docker exec -it cours_hetic-postgres-1 psql -U spotify -d spotify -c \
  "SELECT id, name, label, monthly_listeners FROM artists LIMIT 10;"

# Voir les tracks insérées
docker exec -it cours_hetic-postgres-1 psql -U spotify -d spotify -c \
  "SELECT id, title, duration_ms, genre FROM tracks LIMIT 10;"
```

## 🔄 Tester l'idempotence

L'idempotence signifie que relancer le même DAG 2 fois doit produire le même résultat.

```bash
# 1. Relancer le DAG une 2ème fois
# (Depuis l'UI : Trigger DAG)

# 2. Vérifier les logs du XCom
# Les valeurs "tracks_inserted", "artists_inserted" doivent être identiques

# Ou en SQL :
docker exec -it cours_hetic-postgres-1 psql -U spotify -d spotify -c \
  "SELECT COUNT(*) FROM tracks WHERE created_at > NOW() - INTERVAL '10 minutes';"
```

## 📁 Fichiers de test

Les fichiers JSON de test se trouvent dans `test_data/` :

- `sunset_records.json` : 3 artistes, 3 albums, 4 tracks
- `nightwave_music.json` : 3 artistes, 3 albums, 4 tracks
- `urban_pulse.json` : 3 artistes, 3 albums, 4 tracks

**Total : 9 artistes, 9 albums, 12 tracks**

## 🧹 Arrêter et nettoyer

```bash
# Arrêter les conteneurs
docker-compose down

# Nettoyer aussi les volumes (attention : supprime les données)
docker-compose down -v
```

## 📋 Checklist de validation

Cochez les éléments au fur et à mesure :

- [ ] Docker-compose démarre sans erreur
- [ ] MinIO contient les 3 JSONs (visible dans MinIO Console)
- [ ] DAG `catalog_ingestion_pipeline` se charge sans erreur
- [ ] Premier DAG run s'affiche en **vert** ✅
- [ ] Logs : toutes les 5 tâches ont exécuté avec succès
- [ ] XCom `tracks_inserted` > 0
- [ ] PostgreSQL contient les 12 tracks insérées
- [ ] Deuxième DAG run : same result ✅ (idempotence)
- [ ] Pas d'erreurs en DLQ (ou un nombre prévisible)

## 🐛 Dépannage

### MinIO dit "connexion refusée"

```bash
# Attendre plus longtemps
sleep 30
docker-compose logs minio
```

### PostgreSQL dit "accès refusé"

```bash
# Vérifier que PostgreSQL est démarré
docker-compose ps | grep postgres

# Vérifier la base de données
docker exec -it cours_hetic-postgres-1 psql -U airflow -l
```

### Airflow dit "DAG import error"

```bash
# Vérifier la syntaxe Python du DAG
python3 -m py_compile dags/catalog_ingestion_pipeline.py

# Voir les erreurs complètes
docker-compose logs airflow-init
docker-compose logs airflow-scheduler
```

### Fichiers JSON non uploadés

```bash
# Vérifier que boto3 est installé
pip install boto3

# Upload manuel
python3 upload_to_minio.py
```

## 📚 Ressources

- [Documentation Apache Airflow](https://airflow.apache.org/)
- [Documentation MinIO S3 API](https://docs.min.io/minio/baremetal/reference/minio-server/minio-server.html)
- [Documentation PostgreSQL](https://www.postgresql.org/docs/)

## ✅ Résultats attendus

Une fois le DAG exécuté avec succès :

```
✅ catalog_ingestion_pipeline terminé
DAGRun : 2026-06-02T00:00:00+00:00
Tracks insérées  : 12
Artists insérés  : 9
Erreurs DLQ      : 0
```

**Fin ! 🎉**
