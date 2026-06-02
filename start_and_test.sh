#!/bin/bash
# Script complet pour tester le DAG catalog_ingestion_pipeline

set -e

echo "🚀 Démarrage de l'infrastructure Docker pour Spotify DAGs"
echo "========================================================="
echo ""

# 1. Démarrer docker-compose
echo "1️⃣  Démarrage de docker-compose..."
docker-compose down 2>/dev/null || true
docker-compose up -d

# 2. Attendre que les services soient prêts
echo ""
echo "2️⃣  Attente de démarrage des services (60 secondes)..."
sleep 60

# Vérifier que MinIO est accessible
echo "   Vérification MinIO..."
for i in {1..10}; do
    if curl -s http://localhost:9000/minio/health/live > /dev/null; then
        echo "   ✅ MinIO est prêt"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "   ❌ MinIO n'a pas démarré à temps"
        exit 1
    fi
    echo "   Tentative $i/10..."
    sleep 5
done

# Vérifier que PostgreSQL est accessible
echo "   Vérification PostgreSQL..."
for i in {1..10}; do
    if docker exec cours_hetic-postgres-1 pg_isready -U airflow > /dev/null 2>&1; then
        echo "   ✅ PostgreSQL est prêt"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "   ❌ PostgreSQL n'a pas démarré à temps"
        exit 1
    fi
    echo "   Tentative $i/10..."
    sleep 5
done

# Vérifier que Airflow WebServer est accessible
echo "   Vérification Airflow WebServer..."
for i in {1..10}; do
    if curl -s http://localhost:8080/health > /dev/null; then
        echo "   ✅ Airflow WebServer est prêt"
        break
    fi
    if [ $i -eq 10 ]; then
        echo "   ❌ Airflow WebServer n'a pas démarré à temps"
        exit 1
    fi
    echo "   Tentative $i/10..."
    sleep 5
done

# 3. Uploader les données de test
echo ""
echo "3️⃣  Upload des fichiers JSON dans MinIO..."
if command -v python3 &> /dev/null; then
    python3 upload_to_minio.py
else
    echo "   ⚠️  python3 non trouvé, upload manuel nécessaire"
    echo "   Commande : python3 upload_to_minio.py"
fi

# 4. Afficher les informations d'accès
echo ""
echo "4️⃣  ✅ Infrastructure prête!"
echo ""
echo "📊 Accès aux services :"
echo "  • Airflow UI         : http://localhost:8080"
echo "    Utilisateur : admin"
echo "    Mot de passe: admin"
echo ""
echo "  • MinIO Console      : http://localhost:9001"
echo "    Utilisateur : minioadmin"
echo "    Mot de passe: minioadmin"
echo ""
echo "  • PostgreSQL         : localhost:5432"
echo "    Base de données : spotify"
echo "    Utilisateur     : spotify"
echo "    Mot de passe    : spotify"
echo ""
echo "  • Redis              : localhost:6379"
echo ""
echo "🎯 Prochaines étapes :"
echo "  1. Accédez à http://localhost:8080"
echo "  2. Authentifiez-vous (admin/admin)"
echo "  3. Naviguez vers Dags → catalog_ingestion_pipeline"
echo "  4. Cliquez sur 'Trigger DAG' pour lancer l'exécution"
echo "  5. Attendez que le DAG run s'affiche en vert ✅"
echo ""
echo "📋 Logs en temps réel :"
echo "  • Scheduler : docker-compose logs -f airflow-scheduler"
echo "  • Worker    : docker-compose logs -f airflow-worker"
echo "  • MinIO     : docker-compose logs -f minio"
echo ""
echo "🧹 Arrêter l'infrastructure :"
echo "  docker-compose down"
echo ""
