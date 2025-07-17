import os
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

def upload_all_months_partitioned(year: int, dataset: str, table: str, project: str):
    year = int(year)
    client = bigquery.Client(project=project)
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
    base_table_id = f"{project}.{dataset}.{table}"

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

        # 🔢 Partition mensuelle cible : table$YYYYMM
        partition_suffix = f"{year}{month:02d}"
        partitioned_table_id = f"{base_table_id}${partition_suffix}"

        schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("departement", "STRING"),
            bigquery.SchemaField("temperature", "FLOAT"),
            bigquery.SchemaField("ensoleillement", "FLOAT"),
        ]

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            source_format=bigquery.SourceFormat.PARQUET,
            schema=schema,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.MONTH,
                field="date",
            ),
        )


        temp_parquet = f"/tmp/{year}-{month:02d}-open-meteo.parquet"
        df.to_parquet(temp_parquet, index=False)

        with open(temp_parquet, "rb") as f:
            job = client.load_table_from_file(f, partitioned_table_id, job_config=job_config)
            job.result()
            print(f"✅ Chargé {len(df)} lignes dans la partition {partition_suffix}")

        os.remove(temp_parquet)
