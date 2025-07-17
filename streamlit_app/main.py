import streamlit as st

st.set_page_config(page_title="Tableau de bord météo", layout="wide")

st.title("☀️ Tableau de bord météo en France")
st.markdown("""
Bienvenue dans l'application de visualisation météo !

Utilisez le menu à gauche pour :
- 📍 Visualiser la **carte météo par département**
- 📊 Comparer les **températures moyennes par année**
- 📈 Suivre l'**évolution de la température d’un département** entre deux dates
""")
