import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


def get_dataset_location(project: str, dataset: str) -> str:
    """Retourne la région du dataset BigQuery."""
    temp_client = bigquery.Client(project=project)
    dataset_ref = temp_client.get_dataset(f"{project}.{dataset}")
    return dataset_ref.location


def upload_all_months_partitioned(year: int, dataset: str, table: str, project: str):
    year = int(year)

    # 🔍 Récupérer dynamiquement la région du dataset
    location = get_dataset_location(project, dataset)
    client = bigquery.Client(project=project, location=location)

    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
    base_table_id = f"{project}.{dataset}.{table}"

    # 📌 Schéma standard de la table
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("departement", "STRING"),
        bigquery.SchemaField("temperature", "FLOAT"),
        bigquery.SchemaField("ensoleillement", "FLOAT"),
    ]

    # 🔍 Vérifier si la table existe
    try:
        client.get_table(base_table_id)
        print(f"📁 Table trouvée : {base_table_id}")
    except NotFound:
        print(f"🆕 Table non trouvée. Création de : {base_table_id}")
        table_ref = bigquery.Table(base_table_id, schema=schema)
        table_ref.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="date",
        )
        client.create_table(table_ref)
        print(f"✅ Table créée avec partition mensuelle sur le champ 'date' (région : {location})")

    # 🚀 Chargement des 12 fichiers mensuels
    for month in range(1, 13):
        file_path = os.path.join(data_dir, f"{year}-{month:02d}-open-meteo.csv")
        if not os.path.exists(file_path):
            print(f"⛔ Fichier non trouvé : {file_path}")
            continue

        df = pd.read_csv(file_path)
        if "date" not in df.columns:
            print(f"⛔ Colonne 'date' absente dans {file_path}")
            continue

        # 🔧 Forcer la date au premier jour du mois
        first_day = datetime(year, month, 1).date()
        df["date"] = first_day

        # 🔢 Partition cible : table$YYYYMM
        partition_suffix = f"{year}{month:02d}"
        partitioned_table_id = f"{base_table_id}${partition_suffix}"

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.PARQUET,
            schema=schema,
        )

        temp_parquet = f"/tmp/{year}-{month:02d}-open-meteo.parquet"
        df.to_parquet(temp_parquet, index=False)

        with open(temp_parquet, "rb") as f:
            job = client.load_table_from_file(f, partitioned_table_id, job_config=job_config)
            job.result()
            print(f"✅ Chargé {len(df)} lignes dans la partition {partition_suffix}")

        os.remove(temp_parquet)
