import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys
from google.cloud import bigquery

# --- Rendre streamlit_app importable ---
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from streamlit_app.config import PROJECT_ID, DATASET, TABLE_TEMPERATURE, TABLE_CONSO

st.set_page_config(page_title="Corrélation température / consommation", layout="wide")

@st.cache_data(ttl=3600)
def load_temperature():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            EXTRACT(YEAR FROM date) AS annee,
            EXTRACT(MONTH FROM date) AS mois,
            departement,
            ROUND(AVG(temperature), 2) AS temperature_moyenne,
            ROUND(AVG(ensoleillement), 2) AS ensoleillement_moyen
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_TEMPERATURE}`
        GROUP BY annee, mois, departement
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=3600)
def load_conso():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            annee,
            LPAD(CAST(code_departement AS STRING), 2, '0') AS departement,
            conso_usages_thermosensibles_MWh,
            conso_usages_non_thermosensibles_MWh,
            part_thermosensible,
            thermosensibilite_totale_kWh_DJU,
            dju_a_tr
        FROM `{PROJECT_ID}.{DATASET}.{TABLE_CONSO}`
    """
    return client.query(query).to_dataframe()

st.title("📊 Corrélation entre température et consommation énergétique")

# --- Chargement des données ---
df_temp = load_temperature()
df_conso = load_conso()

# --- Moyenne annuelle des températures ---
df_temp_annual = df_temp.groupby(["annee", "departement"], as_index=False).agg({
    "temperature_moyenne": "mean",
    "ensoleillement_moyen": "mean"
})

# --- Jointure avec la conso ---
df_joined = pd.merge(df_conso, df_temp_annual, on=["annee", "departement"], how="inner")

# --- Sélections ---
annees = sorted(df_joined["annee"].unique())
selected_year = st.selectbox("📅 Année", options=annees, index=len(annees) - 1)

df_filtered = df_joined[df_joined["annee"] == selected_year]

x_options = {
    "Température moyenne (°C)": "temperature_moyenne",
    "Ensoleillement moyen (h)": "ensoleillement_moyen"
}

y_options = {
    "Conso usages thermosensibles (MWh)": "conso_usages_thermosensibles_MWh",
    "Conso usages non thermosensibles (MWh)": "conso_usages_non_thermosensibles_MWh",
    "Part d'usages thermosensibles (%)": "part_thermosensible",
    "Thermosensibilité totale (kWh/DJU)": "thermosensibilite_totale_kWh_DJU"
}

col1, col2 = st.columns(2)
x_label = col1.selectbox("📈 Axe X (variable climatique)", list(x_options.keys()))
y_label = col2.selectbox("📉 Axe Y (variable de consommation)", list(y_options.keys()))

fig = px.scatter(
    df_filtered,
    x=x_options[x_label],
    y=y_options[y_label],
    hover_name="departement",
    labels={
        x_options[x_label]: x_label,
        y_options[y_label]: y_label
    },
    trendline="ols",  # ligne de tendance
)

fig.update_layout(title=f"{y_label} vs {x_label} ({selected_year})", height=600)
st.plotly_chart(fig, use_container_width=True)
