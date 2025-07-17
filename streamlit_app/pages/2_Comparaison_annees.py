import streamlit as st
import pandas as pd
import plotly.express as px
import calendar
import os
import sys
from google.cloud import bigquery

# --- Rendre streamlit_app importable ---
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if ROOT_PATH not in sys.path:
    sys.path.insert(0, ROOT_PATH)

from streamlit_app.config import PROJECT_ID, DATASET, TABLE_TEMPERATURE

st.set_page_config(page_title="Comparaison des températures", layout="wide")

TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE_TEMPERATURE}"

@st.cache_data(ttl=3600)
def load_meteo_data():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            EXTRACT(YEAR FROM date) AS annee,
            EXTRACT(MONTH FROM date) AS mois,
            ROUND(AVG(temperature), 2) AS temperature_moyenne
        FROM `{TABLE_ID}`
        GROUP BY annee, mois
    """
    return client.query(query).to_dataframe()

# --- Titre ---
st.title("📈 Comparaison annuelle des températures moyennes")

# --- Chargement des données ---
df = load_meteo_data()

# Ajout du nom des mois et tri correct
df["mois_nom"] = df["mois"].apply(lambda m: calendar.month_abbr[m])
mois_ordonne = [calendar.month_abbr[i] for i in range(1, 13)]
df["mois_nom"] = pd.Categorical(df["mois_nom"], categories=mois_ordonne, ordered=True)

# Interface utilisateur : sélection d'années
annees = sorted(df["annee"].unique())
annees_selectionnees = st.multiselect(
    "Sélectionnez les années à comparer",
    options=annees,
    default=[annees[-1]]
)

# Filtrage + tri explicite
df_plot = df[df["annee"].isin(annees_selectionnees)].sort_values(["annee", "mois"])

# Graphique
fig = px.line(
    df_plot,
    x="mois_nom",
    y="temperature_moyenne",
    color="annee",
    markers=True,
    labels={
        "mois_nom": "Mois",
        "temperature_moyenne": "Température moyenne (°C)",
        "annee": "Année"
    },
    title="Température moyenne mensuelle par année (France entière)"
)

fig.update_layout(
    xaxis_title="Mois",
    yaxis_title="Température (°C)"
)

st.plotly_chart(fig, use_container_width=True)
