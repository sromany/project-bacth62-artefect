import streamlit as st

st.set_page_config(page_title="Tableau de bord météo")

st.title("☀️ Tableau de bord météo en France")
st.markdown("""
Bienvenue dans l'application de visualisation météo !

Utilisez le menu à gauche pour :
- 📍 Visualiser la **carte météo par département**
- 📊 Comparer les **températures moyennes par année**
- 📈 Suivre l'**évolution de la température d’un département** entre deux dates
- 📍 Visualiser la **carte consommation par département**
- 🔮 Prédire la**consommation d'électricité de la France** en fonction de l'année et de la température
- ⚡️ Prédire la**consommation d'électricité des départements** en fonction de plusieurs paramètres
- ☁️ Observer la **corrélation entre la température ou l'ensoleillement, et la consommation** sur tous les paramètres
""")
