from airflow.decorators import task
from airflow import DAG
import datetime
import os
import subprocess
from urllib.parse import urlparse

# ------------------------------
# CONFIGURATION
# ------------------------------
csv_import_url = (
   "https://data.enedis.fr/api/explore/v2.1/catalog/datasets/consommation-electrique-par-secteur-dactivite-departement/exports/csv?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
)
cloud_storage_path = (
    "gs://projet-data-lake-ghumbert/consommation-electrique-par-secteur-dactivite-departement.csv"
)
local_file_path = "/tmp/data/consommation-electrique-par-secteur-dactivite-departement.csv"

# ------------------------------
# DAG DEFINITION
# ------------------------------
with DAG(
    dag_id='dag_import_enedys',
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["projet-elec"]
) as dag:

    @task(task_id="telechargement_csv_enedis")
    def import_csv():
        os.makedirs('/tmp/data', exist_ok=True)

        # Nettoyage du fichier existant
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
            print(f"🧹 Fichier existant supprimé : {local_file_path}")

        try:
            subprocess.run(
                ['curl', '-s', '-o', local_file_path, csv_import_url],
                check=True
            )
            size = os.path.getsize(local_file_path) / (1024 * 1024)
            print(f"✅ CSV téléchargé depuis Enedis ({size:.2f} MB)")
        except subprocess.CalledProcessError as e:
            print(f"❌ Erreur lors du téléchargement : {e}")
            raise

    @task(task_id="upload_csv_vers_gcs")
    def upload_to_gcs():
        from google.cloud import storage

        parsed = urlparse(cloud_storage_path)
        bucket_name = parsed.netloc
        blob_name = parsed.path.lstrip("/")

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)

        try:
            blob.upload_from_filename(local_file_path)
            print(f"✅ Fichier uploadé vers GCS : {cloud_storage_path}")
        except Exception as e:
            print(f"❌ Erreur lors de l'upload GCS : {e}")
            raise

    @task(task_id="nettoyage_fichier_local")  # (facultatif)
    def cleanup_local_file():
        try:
            os.remove(local_file_path)
            print(f"🧽 Fichier local supprimé : {local_file_path}")
        except FileNotFoundError:
            print("⚠️ Fichier déjà supprimé ou introuvable")
        except Exception as e:
            print(f"❌ Erreur lors du nettoyage : {e}")
            raise

    # Dépendances
    import_csv_task = import_csv()
    upload_to_gcs_task = upload_to_gcs()
    cleanup_task = cleanup_local_file()

    import_csv_task >> upload_to_gcs_task >> cleanup_task
