import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajouter le dossier src au PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from project_batch62_artefact.fetch_open_meteo import run_temperature_extraction
from project_batch62_artefact.upload_partitioned_bq import upload_all_months_partitioned

default_args = {
    "owner": "airflow",
    "retries": 1
}

with DAG(
    dag_id="extract_meteo_partitioned_bq",
    default_args=default_args,
    description="Extraction météo depuis Open-Meteo et chargement dans BigQuery avec partition mensuelle",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["open-meteo", "bigquery", "partition"],
    params={"year": 2024}
) as dag:

    extract_temperature = PythonOperator(
        task_id="extract_temperature",
        python_callable=run_temperature_extraction,
        op_args=["{{ params.year }}"]
    )

    upload_to_bq = PythonOperator(
        task_id="upload_to_bigquery_partitioned",
        python_callable=upload_all_months_partitioned,
        op_args=[
            "{{ params.year }}",
            "open_meteo_dataset",     # → nom du dataset BigQuery
            "meteo",                  # → nom de la table
            "spartan-metric-461712-i9"          # → remplace par l'ID réel de ton projet GCP
        ]
    )

    extract_temperature >> upload_to_bq
