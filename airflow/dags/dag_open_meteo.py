# ./dags/dag_open_meteo.py

import sys
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# Ajouter le dossier 'scripts' au PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts")))

from fetch_open_meteo import run_temperature_extraction  # Assure-toi que le fichier s'appelle bien fetch_open_meteo.py

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": None,  # tu peux ajouter timedelta(minutes=5) si tu veux des délais
}

with DAG(
    dag_id="extract_temperature_prefectures",
    default_args=default_args,
    description="Extraction météo Open-Meteo pour les préfectures françaises",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["meteo", "open-meteo", "departements"],
    params={
        "year": 2024  # valeur par défaut
    }
) as dag:


    extract_temperature = PythonOperator(
        task_id="extract_temperature_2024",
        python_callable=run_temperature_extraction,
        op_args=["{{ params.year }}"]
    )

    extract_temperature
