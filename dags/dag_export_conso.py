import os
import toml
import datetime
import pandas as pd
from airflow import DAG
from airflow.decorators import task
from scipy.stats import linregress

# Charger la config TOML
def load_config():
    config_path = "/opt/airflow/config/app_config.toml"
    return toml.load(config_path)

cfg = load_config()
bucket_path = cfg["gcp"]["bucket_path"]
project_id = cfg["gcp"]["project_id"]
dataset = cfg["gcp"]["dataset"]
table_conso = cfg["gcp"]["table_conso"]
table_reg = cfg["gcp"]["table_reg"]

def get_departement_regression(df, column_name, min_points=2, verbose=True):
    results = []
    for dept, group in df.groupby('code_departement'):
        group = group.dropna(subset=[column_name])
        if len(group) >= min_points:
            slope, intercept, r_value, p_value, std_err = linregress(group['annee'], group[column_name])
            results.append({
                'code_departement': dept,
                'a_slope': slope,
                'b_intercept': intercept,
                'r2': r_value**2,
                'n': len(group)
            })
    df_results = pd.DataFrame(results)
    if verbose:
        print(f"✔ Régression calculée pour {df_results.shape[0]} départements sur '{column_name}'")
    return df_results

with DAG(
    dag_id='dag_export_conso',
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["projet-elec"]
) as dag:

    @task()
    def load_csv():
        file_path = f"{bucket_path}/consommation-electrique-par-secteur-dactivite-departement.csv"
        df = pd.read_csv(file_path, sep=';', low_memory=False)
        print("✅ CSV chargé depuis GCS")
        print(df.head(3))
        return df

    @task()
    def process_data(df):
        colonnes_utiles = [
            "Année", 
            "Code Département",
            "Part thermosensible (%)",
            "Thermosensibilité totale (kWh/DJU)",
            "Conso totale  usages thermosensibles (MWh)",
            "Conso totale  usages non thermosensibles (MWh)",
            "DJU à TR"
        ]
        df_clean = df[colonnes_utiles].rename(columns={
            "Année": "annee",
            "Code Département": "code_departement",
            "Part thermosensible (%)": "part_thermosensible",
            "Thermosensibilité totale (kWh/DJU)": "thermosensibilite_totale_kWh_DJU",
            "Conso totale  usages thermosensibles (MWh)": "conso_usages_thermosensibles_MWh",
            "Conso totale  usages non thermosensibles (MWh)": "conso_usages_non_thermosensibles_MWh",
            "DJU à TR": "dju_a_tr"
        })
        df_clean = df_clean.dropna(subset=["part_thermosensible"])
        print("✅ Données nettoyées")
        print(df_clean.head(3))
        return df_clean
    
    @task()
    def compute_regression(df):
        df_thermo = get_departement_regression(df, 'thermosensibilite_totale_kWh_DJU')
        df_non_thermo = get_departement_regression(df, 'conso_usages_non_thermosensibles_MWh')
        print("✅ Régression calculée")
        return {
            "thermosensibility": df_thermo.to_dict(),
            "non_thermosensible": df_non_thermo.to_dict()
        }

    @task()
    def export_cleaned_data(df_clean):
        df_clean.to_gbq(
            destination_table=f"{project_id}.{dataset}.{table_conso}",
            if_exists="replace"
        )
        print("✅ Données nettoyées exportées vers BigQuery")

    @task()
    def export_regression_models(dfs_dict):
        df_thermo = pd.DataFrame(dfs_dict["thermosensibility"])
        df_non_thermo = pd.DataFrame(dfs_dict["non_thermosensible"])

        if df_thermo.empty or df_non_thermo.empty:
            print("⚠ Pas de données à merger.")
            return

        df_combined = pd.merge(
            df_thermo,
            df_non_thermo,
            on="code_departement",
            suffixes=('_thermosensibility', '_non_thermosensible')
        )
        df_combined.to_gbq(
            destination_table=f"{project_id}.{dataset}.{table_reg}",
            if_exists="replace"
        )
        print("✅ Modèles de régression exportés vers BigQuery")
        print(df_combined.head(3))
        return df_combined

    t1 = load_csv()
    t2 = process_data(t1)
    t3 = compute_regression(t2)
    t4 = export_cleaned_data(t2)
    t5 = export_regression_models(t3)
