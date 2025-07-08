import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

@st.cache_data
def load_data():
    return pd.read_parquet("data/temperature_mensuelle_france_2020_2023.parquet")

df = load_data()

st.title("Dashboard météo des communes françaises")

# Sélecteur de commune
communes = df["commune_code"].unique()
selected_commune = st.selectbox("Choisir une commune", communes)

# Filtrer données par commune
df_commune = df[df["commune_code"] == selected_commune].copy()

# Convertir la colonne 'month' en datetime pour faciliter le tri
df_commune["date"] = pd.to_datetime(df_commune["month"], format="%Y-%m")

st.write(f"### Températures mensuelles pour la commune {selected_commune}")

# Tracer la température par date
fig, ax = plt.subplots(figsize=(10, 5))
ax.plot(df_commune["date"], df_commune["temperature"], marker="o")
ax.set_xlabel("Date")
ax.set_ylabel("Température (°C)")
ax.set_title(f"Température mensuelle pour la commune {selected_commune}")
ax.grid(True)

st.pyplot(fig)
