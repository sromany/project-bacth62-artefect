import streamlit as st
import pandas as pd
import folium
from folium.features import GeoJson, GeoJsonTooltip
import json
import os
from streamlit_folium import st_folium
from google.cloud import bigquery
from calculs import (
    prepare_meteo,
    consommation_annuelle_par_departement
)
from streamlit_app.config import (
    PROJECT_ID, DATASET, TABLE_CONSO, TABLE_REG, TABLE_TEMPERATURE
)

# --------- CHEMIN ABSOLU DU FICHIER GEOJSON ----------
HERE = os.path.dirname(os.path.abspath(__file__))
ASSETS_DIR = os.path.abspath(os.path.join(HERE, "..", "assets"))
GEOJSON_FILENAME = "departements_clean.geojson"
GEOJSON_PATH = os.path.join(ASSETS_DIR, GEOJSON_FILENAME)

# --------- FONCTION UTILITAIRE BQ TABLE REF ----------
def table_ref(table):
    return f"{PROJECT_ID}.{DATASET}.{table}"

# --------- BQ CLIENT ---------
client = bigquery.Client(project=PROJECT_ID)

# --------- DATA ---------
@st.cache_data
def load_table(query):
    return client.query(query).to_dataframe()

df_departement = load_table(f"SELECT * FROM `{table_ref(TABLE_CONSO)}`")
df_models = load_table(f"SELECT * FROM `{table_ref(TABLE_REG)}`")
df_meteo = prepare_meteo(load_table(f"SELECT * FROM `{table_ref(TABLE_TEMPERATURE)}`"))
df_meteo_indexed = df_meteo.set_index(['departement', 'month']).sort_index()

# --------- UI WIDGETS ----------
col1, col2 = st.columns(2)
with col1:
    year = st.number_input("Année", min_value=2020, max_value=2030, value=2026, key="year_input")
with col2:
    temp_offset = st.slider(
        "Hypothèse d'écart de température (°C)",
        min_value=-5.0, max_value=5.0, step=0.5, value=0.0, key="temp_slider"
    )

# --------- CALCULS ----------
df_conso_annuelle = consommation_annuelle_par_departement(year, df_models, df_meteo_indexed, temp_offset=temp_offset)
df_conso_annuelle['code_departement'] = df_conso_annuelle['code_departement'].astype(str).str.zfill(2)

# Total France (TWh)
conso_totale_MWh = df_conso_annuelle["conso_totale_MWh"].sum()
conso_totale_TWh = conso_totale_MWh / 1_000_000

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

# --------- CHARGEMENT GEOJSON ----------
with open(GEOJSON_PATH, encoding="utf-8") as f:
    geojson_data = json.load(f)

# --------- MAP ----------
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
    geo_data=geojson_data,
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

# --------- TOOLTIP PERSONNALISÉE ----------
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

GeoJson(
    geojson_data,
    style_function=lambda x: {
        "fillOpacity": 0,
        "color": "#000000",
        "weight": 0.5
    },
    tooltip=GeoJsonTooltip(fields=["tooltip_html"], aliases=[""], labels=False, sticky=True)
).add_to(m)

# --------- FOLIUM DISPLAY ---------
output = st_folium(m, width="100%", height=600)
if output and "last_active_drawing" in output and output["last_active_drawing"]:
    props = output["last_active_drawing"]["properties"]
    st.session_state.clicked_dep = props.get("code")
    st.session_state.clicked_name = props.get("nom") 
    st.switch_page("pages/6_departement.py")
