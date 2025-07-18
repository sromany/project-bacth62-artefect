import streamlit as st
from google.cloud import bigquery
import pandas as pd
import altair as alt
from calculs import (
    prepare_meteo,
    estimer_conso_complete_mensuelle
)

from streamlit_app.config import (
    PROJECT_ID, DATASET , TABLE_CONSO, TABLE_REG, TABLE_TEMPERATURE
)

def table_ref(table):
    return f"{PROJECT_ID}.{DATASET}.{table}"

# Initialisation
client = bigquery.Client()

# Valeurs par défaut
DEFAULT_DEP_CODE = "75"
DEFAULT_DEP_NAME = "Paris"

# --------------------------
# Choix du département
st.sidebar.header("Département")

input_dep_code = st.sidebar.text_input("Code du département (ex: 75 pour Paris)", value="", max_chars=3)

# Saisie manuelle > clic carte > défaut
if input_dep_code.strip():
    departement_code = input_dep_code.zfill(2)
    departement_name = None  # Nom inconnu
else:
    departement_code = st.session_state.get("clicked_dep", DEFAULT_DEP_CODE)
    departement_name = st.session_state.get("clicked_name", DEFAULT_DEP_NAME)

# Affichage du titre
if departement_name:
    st.title(f"📊 Analyse du département {departement_name} ({departement_code})")
else:
    st.title(f"📊 Analyse du département {departement_code}")

# --------------------------
# Chargement des données
@st.cache_data
def load_table(query):
    return client.query(query).to_dataframe()

code_int = int(departement_code)

# Ici : filtrage direct en SQL comme dans ta version qui marchait bien
df_departement = load_table(
    f"SELECT * FROM `{table_ref(TABLE_CONSO)}` WHERE code_departement = {code_int}"
)

df_models = load_table(f"SELECT * FROM `{table_ref(TABLE_REG)}`")
df_models['code_departement'] = df_models['code_departement'].astype(str).str.zfill(2)

departement_code = str(departement_code).zfill(2)

df_meteo = prepare_meteo(load_table(f"SELECT * FROM `{table_ref(TABLE_TEMPERATURE)}`"))


# --------------------------
# Widgets
year = st.number_input("Année", min_value=2020, max_value=2030, value=2026, key="year_input_departement")
month = st.slider("Mois", min_value=1, max_value=12, value=2, key="month_slider_departement")
temp_offset = st.slider(
    "Hypothèse d'écart de température (°C)",
    min_value=-5.0, max_value=5.0, step=0.5, value=0.0, key="temp_slider_departement"
)

# --------------------------
# Consommation estimée pour le mois sélectionné
df_meteo_dep = df_meteo[
    (df_meteo['departement'] == departement_code) &
    (df_meteo['month'] == month)
]

if df_meteo_dep.empty:
    st.error("Aucune donnée météo disponible pour ce mois.")
else:
    temp_moyenne = df_meteo_dep['temperature'].mean()
    temp_corrigee = temp_moyenne + temp_offset
    nb_jours = df_meteo_dep['date'].nunique()

    conso_mensuelle, info = estimer_conso_complete_mensuelle(
        departement_code,
        year,
        temp_corrigee,
        df_models,
        nb_jours=nb_jours
    )

    if conso_mensuelle is not None:
        st.subheader("📈 Consommation estimée pour le mois")
        st.markdown(f"""
        ### 🧮 {info['total']:,.0f} MWh

        - 🔥 Part thermosensible : **{info['conso_thermo']:,.0f} MWh**
        - 🧊 Part non thermosensible : **{info['conso_non_thermo']:,.0f} MWh**
        - 🌡️ Température moyenne corrigée : **{info['temp_corrigee']}°C**
        - 📏 Seuil de chauffage : **{info['seuil']}°C**
        """)
    else:
        st.warning(f"⚠️ Estimation non disponible pour ce département ({departement_code}).\n{info}")

# --------------------------
# Évolution mensuelle de la consommation
st.subheader(f"📊 Projection de l'évolution de la consommation en {year}")

data_mensuelle = []

for m in range(1, 13):
    df_meteo_mois = df_meteo[
        (df_meteo["departement"] == departement_code) &
        (df_meteo["month"] == m)
    ]
    if df_meteo_mois.empty:
        continue

    temp_moy = df_meteo_mois["temperature"].mean()
    temp_corrigee = temp_moy + temp_offset
    nb_jours = df_meteo_mois["date"].nunique()

    conso, infos = estimer_conso_complete_mensuelle(
        departement_code,
        year,
        temp_corrigee,
        df_models,
        nb_jours=nb_jours
    )

    if conso is None:
        continue

    data_mensuelle.append({
        "mois": m,
        "Conso totale": infos["total"],
        "Thermosensible": infos["conso_thermo"],
        "Non thermosensible": infos["conso_non_thermo"]
    })

df_graph = pd.DataFrame(data_mensuelle)

if df_graph.empty:
    st.info("Aucune donnée disponible pour l'année sélectionnée.")
else:
    df_melt = df_graph.melt(id_vars="mois", var_name="Type", value_name="MWh")

    chart = alt.Chart(df_melt).mark_line(point=True).encode(
        x=alt.X("mois:O", title="Mois"),
        y=alt.Y("MWh:Q", title="Consommation (MWh)"),
        color=alt.Color("Type:N", title="Type de consommation"),
        tooltip=["mois", "Type", "MWh"]
    ).properties(width=700, height=400)

    st.altair_chart(chart, use_container_width=True)

# --------------------------
# Historique de consommation annuelle
st.subheader("📚 Historique annuel de la consommation réelle")

expected_cols = [
    "annee",
    "conso_usages_thermosensibles_MWh",
    "conso_usages_non_thermosensibles_MWh"
]

if not all(col in df_departement.columns for col in expected_cols):
    st.warning("Colonnes manquantes dans la table pour afficher l'historique annuel.")
else:
    df_historique = df_departement.copy()
    df_historique = df_historique.sort_values("annee")
    df_historique["Conso totale"] = (
        df_historique["conso_usages_thermosensibles_MWh"] +
        df_historique["conso_usages_non_thermosensibles_MWh"]
    )

    df_melt_hist = df_historique.melt(
        id_vars="annee",
        value_vars=["Conso totale", "conso_usages_thermosensibles_MWh", "conso_usages_non_thermosensibles_MWh"],
        var_name="Type",
        value_name="MWh"
    )

    df_melt_hist["Type"] = df_melt_hist["Type"].replace({
        "conso_usages_thermosensibles_MWh": "Thermosensible",
        "conso_usages_non_thermosensibles_MWh": "Non thermosensible"
    })

    chart_hist = alt.Chart(df_melt_hist).mark_line(point=True).encode(
        x=alt.X("annee:O", title="Année"),
        y=alt.Y("MWh:Q", title="Consommation (MWh)"),
        color=alt.Color("Type:N", title="Type de consommation"),
        tooltip=["annee", "Type", "MWh"]
    ).properties(width=700, height=400)

    st.altair_chart(chart_hist, use_container_width=True)
