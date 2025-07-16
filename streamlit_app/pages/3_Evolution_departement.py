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

st.set_page_config(page_title="Évolution départementale", layout="wide")

# --- CONFIGURATION ---
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE_TEMPERATURE}"

@st.cache_data(ttl=3600)
def load_data():
    client = bigquery.Client(project=PROJECT_ID)
    query = f"""
        SELECT
            date,
            departement,
            ROUND(AVG(temperature), 2) AS temperature_moyenne,
            ROUND(AVG(ensoleillement), 2) AS ensoleillement_moyen
        FROM `{TABLE_ID}`
        GROUP BY date, departement
    """
    df = client.query(query).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])
    df["annee"] = df["date"].dt.year
    df["mois"] = df["date"].dt.month
    return df

# --- TITRE ---
st.title("📊 Évolution des données météo dans un département")

# --- CHARGEMENT DES DONNÉES ---
df = load_data()

# --- SÉLECTION DU DÉPARTEMENT ET DES DATES ---
departements = sorted(df["departement"].unique())
selected_dep = st.selectbox("Choisissez un département", departements)

annees_disponibles = sorted(df["annee"].unique())
mois_disponibles = list(range(1, 13))
mois_noms = {i: calendar.month_name[i] for i in mois_disponibles}

col1, col2 = st.columns(2)
with col1:
    annee_debut = st.selectbox("Année de début", annees_disponibles, index=0)
    mois_debut = st.selectbox("Mois de début", mois_disponibles, format_func=lambda m: mois_noms[m])
with col2:
    annee_fin = st.selectbox("Année de fin", annees_disponibles, index=len(annees_disponibles) - 1)
    mois_fin = st.selectbox("Mois de fin", mois_disponibles, format_func=lambda m: mois_noms[m])

# Indicateur à afficher
indicateur_label = st.radio(
    "Indicateur à afficher",
    ["Température moyenne (°C)", "Ensoleillement moyen (h)"]
)
selected_column = "temperature_moyenne" if "Température" in indicateur_label else "ensoleillement_moyen"

# --- FILTRAGE DES DONNÉES ---
date_start = pd.Timestamp(year=annee_debut, month=mois_debut, day=1)
date_end = pd.Timestamp(year=annee_fin, month=mois_fin, day=1) + pd.offsets.MonthEnd(0)

df_filtered = df[
    (df["date"] >= date_start) &
    (df["date"] <= date_end)
].copy()

# Courbe du département sélectionné
df_dep = df_filtered[df_filtered["departement"] == selected_dep].sort_values("date")

# Moyenne nationale
df_national = (
    df_filtered.groupby("date")[selected_column]
    .mean()
    .reset_index()
    .sort_values("date")
)

# --- TRACÉ ---
fig = px.line(
    df_dep,
    x="date",
    y=selected_column,
    title=f"Évolution de {indicateur_label.lower()} dans le département {selected_dep}",
    markers=True,
    labels={
        "date": "Date",
        selected_column: indicateur_label
    }
)

# Ajout de la moyenne nationale
fig.add_scatter(
    x=df_national["date"],
    y=df_national[selected_column],
    mode="lines",
    name="Moyenne nationale",
    line=dict(color="gray", dash="dot")
)

fig.update_layout(
    xaxis_title="Date",
    yaxis_title=indicateur_label,
    xaxis=dict(tickformat="%b %Y", tickangle=45),
    legend=dict(y=1.05, orientation="h")
)

st.plotly_chart(fig, use_container_width=True)

# --- BOUTON EXPORT CSV ---
st.download_button(
    label="📥 Télécharger les données CSV",
    data=df_dep[["date", selected_column]].to_csv(index=False).encode("utf-8"),
    file_name=f"{selected_dep}_{selected_column}.csv",
    mime="text/csv"
)
