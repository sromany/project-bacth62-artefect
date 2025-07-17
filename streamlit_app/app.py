import streamlit as st
import pandas as pd
import folium
from folium.features import GeoJson, GeoJsonTooltip
import json
from streamlit_folium import st_folium
import plotly.express as px
import branca.colormap as cm
from google.cloud import bigquery
from calculs import (
    prepare_meteo,
    estimer_conso_complete_mensuelle,
    consommation_annuelle_par_departement
)

# --------------------------
# CONFIG
geojson_url = "streamlit/assets/departements_clean.geojson"
client = bigquery.Client()

# --------------------------
# UI
st.markdown("""
    <style>
        iframe.stCustomComponentV1 {
            height: 600px !important;
        }
    </style>
""", unsafe_allow_html=True)

st.title("Prévision de la consommation électrique des ménages en France")

# --------------------------
# Chargement des données
@st.cache_data
def load_table(query):
    return client.query(query).to_dataframe()

df_departement = load_table("""
    SELECT * 
    FROM graphic-bonus-461713-m5.sql62_local.conso_elec_departement
""")
df_models = load_table("""
    SELECT * 
    FROM graphic-bonus-461713-m5.sql62_local.conso_elec_regression_models
""")
df_meteo = prepare_meteo(load_table("""
    SELECT * 
    FROM spartan-metric-461712-i9.open_meteo_dataset.meteo
"""))
df_meteo_indexed = df_meteo.set_index(['departement', 'month']).sort_index()

# --------------------------
# Widgets
# departement_code = st.sidebar.number_input("Code département", min_value=1, max_value=95, value=90)
# year = st.sidebar.number_input("Année", min_value=2020, max_value=2030, value=2026)
# temp_offset = st.sidebar.slider("Hypothèse d'écart de température (°C)", min_value=-5.0, max_value=5.0, step=0.5, value=0.0)
# month = st.sidebar.slider("Mois", min_value=1, max_value=12, value=2)

col1, col2 = st.columns(2)

with col1:
    year = st.number_input("Année", min_value=2020, max_value=2030, value=2026, key="year_input")

with col2:
    temp_offset = st.slider(
        "Hypothèse d'écart de température (°C)",
        min_value=-5.0, max_value=5.0, step=0.5, value=0.0, key="temp_slider"
    )

# --------------------------
# Température du mois
# @st.cache_data
# def get_temp_moyenne_mois(df_meteo, departement_code, month):
#     code_str = f"{departement_code:02d}"
#     df_mois = df_meteo[
#         (df_meteo['departement'] == code_str) &
#         (df_meteo['month'] == month)
#     ]
#     return df_mois['temperature'].mean()

# temp_moyenne_mois = get_temp_moyenne_mois(df_meteo, departement_code, month)

# --------------------------
# Conso annuelle
df_conso_annuelle = consommation_annuelle_par_departement(year, df_models, df_meteo_indexed, temp_offset=temp_offset)
df_conso_annuelle['code_departement'] = df_conso_annuelle['code_departement'].astype(str).str.zfill(2)

# Calcul de la conso totale France en TWh
conso_totale_MWh = df_conso_annuelle["conso_totale_MWh"].sum()
conso_totale_TWh = conso_totale_MWh / 1_000_000

# Formulation conditionnelle
if temp_offset == 0:
    phrase_temp = ""
elif temp_offset > 0:
    phrase_temp = f" si la température moyenne est supérieure de {abs(temp_offset):.1f} °C à la normale"
else:
    phrase_temp = f" si la température moyenne est inférieure de {abs(temp_offset):.1f} °C à la normale"

st.markdown(f"""
<div style='font-size:24px; font-weight:bold; margin-bottom:20px'>
En {year}, la consommation des ménages français sera de <span style='color:#0066cc'>{conso_totale_TWh:.2f} TWh</span>{phrase_temp}.
</div>
""", unsafe_allow_html=True)

# --------------------------
# Affichage estimation conso mensuelle
# conso_mensuelle, info = estimer_conso_complete_mensuelle(departement_code, year, temp_moyenne_mois, df_models)

# --------------------------
# Carte Folium
m = folium.Map(
    location=[46.8, 2.5], 
    zoom_start=6, 
    tiles=None,
    control_scale=False,
    zoom_control=False,
    dragging=False,
    scrollWheelZoom=False,
    doubleClickZoom=False,
    touchZoom=False,
    prefer_canvas=True
)
folium.Rectangle(
    bounds=[[39.5, -7.0], [53.5, 11.0]],
    color=None,
    fill=True,
    fill_color="white",
    fill_opacity=1,
    interactive=False
).add_to(m)
folium.Choropleth(
    geo_data=geojson_url,
    data=df_conso_annuelle,
    columns=["code_departement", "conso_totale_MWh"],
    key_on="feature.properties.code",
    fill_color="YlOrRd",
    fill_opacity=0.7,
    line_opacity=0.2,
    nan_fill_color="white",
    highlight=True
).add_to(m)
m.get_root().html.add_child(folium.Element("<style>.legend { display: none !important; }</style>"))

# --------------------------
# Ajout des infobulles personnalisées
with open(geojson_url, encoding="utf-8") as f:
    geojson_data = json.load(f)

conso_dict = df_conso_annuelle.set_index("code_departement")["conso_totale_MWh"].to_dict()

for feature in geojson_data["features"]:
    code = feature["properties"]["code"]
    nom = feature["properties"]["nom"]
    conso = conso_dict.get(code)
    if conso is not None:
        if conso >= 1_000_000:
            value = f"{conso / 1_000_000:.2f} TWh"
        elif conso >= 1_000:
            value = f"{conso / 1_000:.0f} GWh"
        else:
            value = f"{int(conso):,} MWh".replace(",", " ")
        feature["properties"]["tooltip_html"] = f"<b>{nom}</b><br/>{value}"
    else:
        feature["properties"]["tooltip_html"] = f"<b>{nom}</b><br/>Données manquantes"

# Création d’un seul GeoJson avec tooltip automatique
GeoJson(
    geojson_data,
    style_function=lambda x: {
        "fillOpacity": 0,
        "color": "#000000",
        "weight": 0.5
    },
    tooltip=GeoJsonTooltip(fields=["tooltip_html"], aliases=[""], labels=False, sticky=True)
).add_to(m)

# --------------------------
# Clic sur département
output = st_folium(m, width="100%", height=600)
clicked_dep = None
if output and "last_active_drawing" in output and output["last_active_drawing"]:
    props = output["last_active_drawing"]["properties"]
    st.session_state.clicked_dep = props.get("code")
    st.session_state.clicked_name = props.get("nom") 
    st.switch_page("pages/6_departement.py")
