import streamlit as st
import pandas as pd
from google.cloud import bigquery

from streamlit_app.config import (
    PROJECT_ID, DATASET , TABLE_CONSO, TABLE_REG, TABLE_TEMPERATURE
)

def table_ref(table):
    return f"{PROJECT_ID}.{DATASET}.{table}"

# Initialisation
client = bigquery.Client()

# --------------------
@st.cache_data
def load_table(query):
    return client.query(query).to_dataframe()
# --------------------
@st.cache_data
def prepare_meteo(df):
    df = df.copy()
    df['month'] = pd.to_datetime(df['date']).dt.month.astype(int)
    return df
# --------------------
df_departement = load_table(f"SELECT * FROM {table_ref(TABLE_CONSO)}")
df_models = load_table(f"SELECT * FROM {table_ref(TABLE_REG)}")
df_meteo = prepare_meteo(load_table(f"SELECT * FROM {table_ref(TABLE_TEMPERATURE)}"))

df_meteo = prepare_meteo(df_meteo)


@st.cache_data
def calculer_conso_total(df):
    df_total = df.copy()
    df_total["conso_total_MWh"] = df_total["conso_usages_thermosensibles_MWh"] + df_total["conso_usages_non_thermosensibles_MWh"]
    df_agg = df_total.groupby("annee")["conso_total_MWh"].sum().reset_index()
    df_agg["conso_total_TWh"] = df_agg["conso_total_MWh"] / 1_000_000
    return df_agg[["annee", "conso_total_TWh"]].sort_values("annee", ascending=False)

df_agg = calculer_conso_total(df_departement)

@st.cache_data
def calculer_temp_annual(df):
    df_temp = df.copy()
    df_temp['annee'] = pd.to_datetime(df_temp['date']).dt.year
    df_dep_annual_mean = df_temp.groupby(['annee', 'departement'])['temperature'].mean().reset_index()
    df_annual_mean = df_dep_annual_mean.groupby('annee')['temperature'].mean().reset_index()
    return df_annual_mean.rename(columns={'temperature': 'temperature_moyenne'})

df_annual_mean = calculer_temp_annual(df_meteo)
st.subheader("📄 Table conso_elec_departement")
st.dataframe(df_departement)

st.subheader("🧮 Consommation totale (TWh) par année")
st.dataframe(df_agg)

st.subheader("📈 Table conso_elec_regression_models")
st.dataframe(df_models)

st.subheader("🌦️ Données météo triées")
df_meteo_sorted = df_meteo.sort_values(by=['departement', 'date'])[['date', 'departement', 'month', 'temperature']]
st.dataframe(df_meteo_sorted)

st.subheader("🌡️ Température moyenne annuelle")
st.dataframe(df_annual_mean)
