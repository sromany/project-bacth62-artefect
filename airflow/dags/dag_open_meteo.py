import os
import sys
from datetime import datetime

import toml
from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajouter le dossier src au PYTHONPATH pour les imports custom
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from project_batch62_artefact.fetch_open_meteo import run_temperature_extraction
from project_batch62_artefact.upload_partitioned_bq import upload_all_months_partitioned

# Chargement de la configuration TOML
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "app_config.toml")
    return toml.load(config_path)

cfg = load_config()
project_id = cfg["gcp"]["project_id"]
dataset = cfg["gcp"]["dataset"]
table_temperature = cfg["gcp"]["table_temperature"]

default_args = {
    "owner": "airflow",
    "retries": 1,
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
        op_args=["{{ params.year }}"],
    )

    upload_to_bq = PythonOperator(
        task_id="upload_to_bigquery_partitioned",
        python_callable=upload_all_months_partitioned,
        op_args=[
            "{{ params.year }}",
            dataset,
            table_temperature,
            project_id
        ],
    )

    extract_temperature >> upload_to_bq
