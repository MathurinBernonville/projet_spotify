#!/usr/bin/env python3
"""
Script pour uploader les fichiers JSON de test dans MinIO.
À exécuter après que docker-compose soit démarré.

Usage:
    python3 upload_to_minio.py
"""

import boto3
import json
import sys
from pathlib import Path

# Configuration MinIO
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_BUCKET = "labels-raw"

# Dossier contenant les fichiers de test
TEST_DATA_DIR = Path(__file__).parent / "test_data"

def upload_to_minio():
    """Upload les fichiers JSON dans MinIO."""
    
    # Créer le client S3/MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name='us-east-1'
    )
    
    # Files à uploader
    json_files = [
        'sunset_records.json',
        'nightwave_music.json',
        'urban_pulse.json'
    ]
    
    print(f"🔌 Connexion à MinIO : {MINIO_ENDPOINT}")
    print(f"📦 Bucket cible : {MINIO_BUCKET}")
    print(f"📁 Dossier source : {TEST_DATA_DIR}")
    print()
    
    # Vérifier que le dossier existe
    if not TEST_DATA_DIR.exists():
        print(f"❌ Dossier {TEST_DATA_DIR} n'existe pas!")
        return False
    
    uploaded_count = 0
    
    for file_name in json_files:
        file_path = TEST_DATA_DIR / file_name
        
        if not file_path.exists():
            print(f"⚠️  Fichier manquant : {file_path}")
            continue
        
        try:
            # Lire le fichier
            with open(file_path, 'r') as f:
                file_content = f.read()
            
            # Valider le JSON
            json.loads(file_content)
            
            # Uploader dans MinIO
            s3_client.put_object(
                Bucket=MINIO_BUCKET,
                Key=file_name,
                Body=file_content.encode('utf-8'),
                ContentType='application/json'
            )
            
            print(f"✅ Uploadé : {file_name}")
            uploaded_count += 1
            
        except json.JSONDecodeError as e:
            print(f"❌ JSON invalide : {file_name} - {str(e)}")
        except Exception as e:
            print(f"❌ Erreur upload {file_name} : {str(e)}")
    
    print()
    if uploaded_count == len(json_files):
        print(f"✅ {uploaded_count} fichiers uploadés avec succès!")
        return True
    else:
        print(f"⚠️  {uploaded_count}/{len(json_files)} fichiers uploadés")
        return uploaded_count > 0

if __name__ == "__main__":
    try:
        success = upload_to_minio()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n⏹️  Opération annulée")
        sys.exit(1)
