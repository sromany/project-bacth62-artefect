import os
import pandas as pd
from google.cloud import bigquery

def upload_all_months_partitioned(year: int, dataset: str, table: str, project: str):
    client = bigquery.Client(project=project)
    data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "data"))
    full_table_id = f"{project}.{dataset}.{table}"

    all_rows = []

    for month in range(1, 13):
        file_path = os.path.join(data_dir, f"{year}-{month:02d}-open-meteo.csv")
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            df["date"] = pd.to_datetime(df["date"])
            all_rows.append(df)
        else:
            print(f"⛔ Fichier non trouvé : {file_path}")

    if not all_rows:
        print("⛔ Aucun fichier à charger.")
        return

    full_df = pd.concat(all_rows)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        source_format=bigquery.SourceFormat.PARQUET,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.MONTH,
            field="date"
        ),
        autodetect=True
    )

    job = client.load_table_from_dataframe(full_df, full_table_id, job_config=job_config)
    job.result()

    print(f"✅ Chargé {len(full_df)} lignes dans {full_table_id}")
