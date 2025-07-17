import streamlit as st
import pandas as pd
import plotly.express as px
import json
import calendar
import os
import sys
from google.cloud import bigquery

# --- Rendre streamlit_app importable ---
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from streamlit_app.config import PROJECT_ID, DATASET, TABLE_TEMPERATURE

st.set_page_config(page_title="Carte météo interactive", layout="wide")

# --- CONFIGURATION ---
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE_TEMPERATURE}"
HERE = os.path.dirname(os.path.abspath(__file__))
GEOJSON_PATH = os.path.join("/opt/streamlit_app", "assets", "departements.geojson")
@st.cache_data(ttl=3600)
def load_geojson():
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)



def get_departement_mapping(geojson):
    return {
        feature["properties"]["code"]: feature["properties"]["nom"]
        for feature in geojson["features"]
    }

@st.cache_data(ttl=3600)
def load_meteo_data():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            EXTRACT(YEAR FROM date) AS annee,
            EXTRACT(MONTH FROM date) AS mois,
            departement,
            ROUND(AVG(temperature), 2) AS temperature_moyenne,
            ROUND(AVG(ensoleillement), 2) AS ensoleillement_moyen
        FROM `{TABLE_ID}`
        GROUP BY annee, mois, departement
    """
    return client.query(query).to_dataframe()

st.title("🗺️ Carte météo par département")

if st.button("🔄 Rafraîchir les données"):
    st.cache_data.clear()
    st.rerun()

geojson = load_geojson()
df = load_meteo_data()
departement_mapping = get_departement_mapping(geojson)
df["nom_departement"] = df["departement"].map(departement_mapping)

annees = sorted(df["annee"].unique())
mois_possibles = sorted(df["mois"].unique())
mois_noms = {i: calendar.month_name[i].capitalize() for i in mois_possibles}

indicateurs = {
    "Température moyenne (°C)": {
        "col": "temperature_moyenne",
        "colorscale": "RdBu_r"
    },
    "Ensoleillement moyen (h)": {
        "col": "ensoleillement_moyen",
        "colorscale": ["#001f3f", "#06B5A0", "#FFDC00"]
    }
}

col1, col2 = st.columns(2)
selected_year = col1.selectbox("📆 Année", annees, index=len(annees) - 1)
selected_month = col2.selectbox("🗓️ Mois", options=mois_possibles, format_func=lambda m: mois_noms[m])
selected_indicator_label = st.radio("📊 Indicateur à afficher", list(indicateurs.keys()))
selected_column = indicateurs[selected_indicator_label]["col"]
selected_colorscale = indicateurs[selected_indicator_label]["colorscale"]

df_filtered = df[(df["annee"] == selected_year) & (df["mois"] == selected_month)]

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
    labels={selected_column: selected_indicator_label},
)

fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
st.plotly_chart(fig, use_container_width=True)
