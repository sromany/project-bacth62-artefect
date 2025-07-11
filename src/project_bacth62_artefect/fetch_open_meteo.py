import requests
import pandas as pd
import sys
import os
import time
from collections import defaultdict

API_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
TIMEZONE = "Europe/Paris"
OUTPUT_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data"))
SLEEP_BETWEEN_CALLS = 3  # seconds
MAX_RETRIES = 3  # nombre maximum de tentatives pour un département

def get_prefectures():
    response = requests.get("https://geo.api.gouv.fr/departements")
    response.raise_for_status()
    departements = response.json()
    prefectures = []

    for dpt in departements:
        code_dep = dpt["code"]
        r = requests.get(
            "https://geo.api.gouv.fr/communes",
            params={
                "codeDepartement": code_dep,
                "fields": "nom,code,centre,population",
                "format": "json",
                "geometry": "centre"
            }
        )
        r.raise_for_status()
        results = r.json()
        communes = [c for c in results if "population" in c and c["population"] is not None]
        if not communes:
            continue
        commune = max(communes, key=lambda x: x["population"])
        coords = commune.get("centre", {}).get("coordinates")
        if coords:
            lon, lat = coords
            prefectures.append({
                "departement": code_dep,
                "code_insee": commune["code"],
                "nom": commune["nom"],
                "lat": lat,
                "lon": lon
            })

    return prefectures

def fetch_weather(commune, year):
    params = {
        "latitude": commune["lat"],
        "longitude": commune["lon"],
        "start_date": f"{year}-01-01",
        "end_date": f"{year}-12-31",
        "daily": "temperature_2m_max,sunshine_duration",
        "timezone": TIMEZONE
    }
    r = requests.get(API_URL, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def process_weather(commune, response):
    df = pd.DataFrame({
        "date": pd.to_datetime(response["daily"]["time"]),
        "temperature": response["daily"]["temperature_2m_max"],
        "ensoleillement": response["daily"]["sunshine_duration"]
    })
    df["mois"] = df["date"].dt.month

    result = df.groupby("mois").agg({
        "temperature": "mean",
        "ensoleillement": "mean"
    }).round(2).reset_index()

    result["departement"] = commune["departement"]
    return result[["mois", "departement", "temperature", "ensoleillement"]]

def run_temperature_extraction(year):
    year = int(year)
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    prefectures = get_prefectures()
    print(f"✅ {len(prefectures)} préfectures récupérées.")

    all_months = defaultdict(list)
    departements_traités = set()

    for commune in prefectures:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = fetch_weather(commune, year)
                result = process_weather(commune, response)
                for _, row in result.iterrows():
                    all_months[int(row["mois"])].append({
                        "departement": row["departement"],
                        "temperature": row["temperature"],
                        "ensoleillement": row["ensoleillement"]
                    })
                departements_traités.add(commune["departement"])
                break  # sortie du retry si succès
            except Exception as e:
                print(f"❌ Tentative {attempt} échouée pour {commune['nom']} ({commune['departement']}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(5)
                else:
                    print(f"⛔ Abandon pour {commune['nom']} ({commune['departement']}) après {MAX_RETRIES} tentatives.")

        time.sleep(SLEEP_BETWEEN_CALLS)

    # Sauvegarde des fichiers mensuels
    for month, rows in all_months.items():
        output_file = os.path.join(OUTPUT_FOLDER, f"{year}-{month:02d}-open-meteo.csv")
        pd.DataFrame(rows).to_csv(output_file, index=False, float_format="%.2f")
        print(f"✅ Fichier sauvegardé : {output_file}")

    # Vérifie les départements manquants
    tous_deps = {c["departement"] for c in prefectures}
    manquants = sorted(tous_deps - departements_traités)
    if manquants:
        print(f"📉 Départements manquants ({len(manquants)}): {', '.join(manquants)}")
    else:
        print("✅ Tous les départements ont été traités avec succès.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python fetch_open_meteo.py <year>")
        sys.exit(1)

    year = sys.argv[1]
    run_temperature_extraction(year)
