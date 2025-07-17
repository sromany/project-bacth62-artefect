import streamlit as st
import pandas as pd
import plotly.express as px
import json
import os
import sys
from google.cloud import bigquery

# --- Rendre streamlit_app importable ---
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from streamlit_app.config import PROJECT_ID, DATASET, TABLE_CONSO

st.set_page_config(page_title="Carte consommation énergétique", layout="wide")

# --- CONFIGURATION ---
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE_CONSO}"
GEOJSON_PATH = "streamlit_app/assets/departements.geojson"

@st.cache_data
def load_geojson():
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

def get_departement_mapping(geojson):
    return {
        feature["properties"]["code"]: feature["properties"]["nom"]
        for feature in geojson["features"]
    }

@st.cache_data(ttl=3600)
def load_conso_data():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            annee,
            LPAD(CAST(code_departement AS STRING), 2, '0') AS departement,
            part_thermosensible,
            thermosensibilite_totale_kWh_DJU,
            conso_usages_thermosensibles_MWh,
            conso_usages_non_thermosensibles_MWh,
            dju_a_tr
        FROM `{TABLE_ID}`
    """
    return client.query(query).to_dataframe()

# --- TITRE ---
st.title("⚡ Carte de la consommation énergétique par département")

if st.button("🔄 Rafraîchir les données"):
    st.cache_data.clear()
    st.rerun()

geojson = load_geojson()
df = load_conso_data()
departement_mapping = get_departement_mapping(geojson)
df["nom_departement"] = df["departement"].map(departement_mapping)

# --- SÉLECTIONS ---
annees = sorted(df["annee"].unique())
selected_year = st.selectbox("📅 Année", annees, index=len(annees) - 1)

indicateurs = {
    "Part d'usages thermosensibles (%)": {
        "col": "part_thermosensible",
        "colorscale": "YlOrRd"
    },
    "Conso usages thermosensibles (MWh)": {
        "col": "conso_usages_thermosensibles_MWh",
        "colorscale": "Oranges"
    },
    "Conso usages non thermosensibles (MWh)": {
        "col": "conso_usages_non_thermosensibles_MWh",
        "colorscale": "Blues"
    },
    "Thermosensibilité totale (kWh/DJU)": {
        "col": "thermosensibilite_totale_kWh_DJU",
        "colorscale": "Viridis"
    }
}

selected_label = st.radio("📊 Indicateur à afficher", list(indicateurs.keys()))
selected_column = indicateurs[selected_label]["col"]
selected_colorscale = indicateurs[selected_label]["colorscale"]

df_filtered = df[df["annee"] == selected_year]

fig = px.choropleth(
    df_filtered,
    geojson=geojson,
    locations="departement",
    featureidkey="properties.code",
    color=selected_column,
    hover_name="nom_departement",
    hover_data={"departement": True, selected_column: True},
    color_continuous_scale=selected_colorscale,
    range_color=(df[selected_column].min(), df[selected_column].max()),
    scope="europe",
    labels={selected_column: selected_label},
)

fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
st.plotly_chart(fig, use_container_width=True)
