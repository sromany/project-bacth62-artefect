import os
import sys
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import toml

# Ajouter le dossier src au PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "src")))

from project_batch62_artefact.fetch_open_meteo import run_temperature_extraction
from project_batch62_artefact.upload_partitioned_bq import upload_all_months_partitioned

# Charger la config TOML
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "app_config.toml")
    return toml.load(config_path)

cfg = load_config()
project = cfg["gcp"]["project_id"]
dataset = cfg["gcp"]["dataset"]
table = cfg["gcp"]["table"]

# Définition du DAG
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
    params={"years": [2020, 2021, 2022, 2023, 2024]}
) as dag:

    with TaskGroup("extract_and_upload_group") as extract_and_upload_group:
        for year in dag.params["years"]:
            extract_task = PythonOperator(
                task_id=f"extract_temperature_{year}",
                python_callable=run_temperature_extraction,
                op_args=[year]
            )

            upload_task = PythonOperator(
                task_id=f"upload_to_bigquery_partitioned_{year}",
                python_callable=upload_all_months_partitioned,
                op_args=[
                    year,
                    dataset,
                    table,
                    project
                ]
            )

            extract_task >> upload_task
