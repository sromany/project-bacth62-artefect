from airflow.decorators import task
from airflow import DAG
import pandas as pd
import datetime
from airflow.providers.standard.operators.empty import EmptyOperator
from scipy.stats import linregress

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
        df = pd.read_csv(
            "gs://projet-data-lake-ghumbert/consommation-electrique-par-secteur-dactivite-departement.csv",
            sep=';',
            low_memory=False
        )
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
        df_clean = df[colonnes_utiles]
        #rename columns for bigquery compatibility
        df_clean = df_clean.rename(columns={
            "Année": "annee",
            "Code Département": "code_departement",
            "Part thermosensible (%)": "part_thermosensible",
            "Thermosensibilité totale (kWh/DJU)": "thermosensibilite_totale_kWh_DJU",
            "Conso totale  usages thermosensibles (MWh)": "conso_usages_thermosensibles_MWh",
            "Conso totale  usages non thermosensibles (MWh)": "conso_usages_non_thermosensibles_MWh",
            "DJU à TR": "dju_a_tr"
        })
        df_clean = df_clean.dropna(subset=["part_thermosensible"])
        print("✅ CSV nétoyé et colonnes utiles sélectionnées")
        print(df_clean.head(3))
        return df_clean
    
    @task()
    def compute_regression(df):
        df_thermosensibility_models = get_departement_regression(df, 'thermosensibilite_totale_kWh_DJU')
        print("✅ Modèles de régression pour la thermosensibilité calculés")
        df_non_thermosensible_conso_models = get_departement_regression(df, 'conso_usages_non_thermosensibles_MWh')
        print("✅ Modèles de régression pour la consommation non thermosensible calculés")

        print(df_thermosensibility_models.head(3))
        print(df_non_thermosensible_conso_models.head(3))
        return {
            "thermosensibility": df_thermosensibility_models.to_dict(),
            "non_thermosensible": df_non_thermosensible_conso_models.to_dict()
        }

    # Export cleaned data to BigQuery
    @task()
    def export_cleaned_data(df_clean):
        df_clean.to_gbq(
            destination_table="graphic-bonus-461713-m5.sql62_local.conso_elec_departement",
            if_exists="replace"
        )
        print("✅ Données nettoyées exportées vers BigQuery")

    # Export regression models to BigQuery by department in a single table
    @task()
    def export_regression_models(dfs_dict):
        # Combine the two regression model DataFrames
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
            destination_table="graphic-bonus-461713-m5.sql62_local.conso_elec_regression_models",
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