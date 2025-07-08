#!/usr/bin/env python3
"""
Collecte mensuelle des températures pour toutes les communes du département 77.
"""

import asyncio
import pandas as pd
from pathlib import Path
from loguru import logger
import sys

# Setup chemin
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root / "src"))

from data.collectors.meteo_france import collect_meteo_data_for_communes
from config import settings

# ─────────── 1. Charger les communes du 77 ───────────

def get_communes_77():
    """Charge les communes du département 77 depuis un fichier CSV local."""
    path = project_root / "data" / "geo" / "communes-france.csv"
    df = pd.read_csv(path, dtype=str)
    df_77 = df[df["code_departement"] == "77"]
    return df_77["code_commune"].tolist()

# ─────────── 2. Collecte météo ───────────

async def collect_meteo_77(start_year=2020, end_year=2023):
    
    commune_codes = get_communes_77()
    print(f"Communes à traiter ({len(commune_codes)}): {commune_codes}")
    logger.info(f"📍 {len(commune_codes)} communes à traiter pour le 77")

    df = await collect_meteo_data_for_communes(commune_codes, start_year, end_year)

    if df is None or df.empty:
        logger.warning("❌ Aucune donnée récupérée")
        return

    # Agréger par mois
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.to_period("M")
    monthly_avg = (
        df.groupby(["commune_code", "month"])["temperature"]
        .mean()
        .reset_index()
        .sort_values(["commune_code", "month"])
    )

    # Sauvegarde
    output_path = settings.RAW_DATA_PATH / "meteo" / "dept_77"
    output_path.mkdir(parents=True, exist_ok=True)
    filename = f"temperature_mensuelle_77_{start_year}_{end_year}.parquet"
    monthly_avg.to_parquet(output_path / filename)

    logger.success(f"✅ Températures mensuelles sauvegardées dans {filename}")
    print(monthly_avg.groupby("commune_code").head(12))  # affichage de test

# ─────────── 3. Exécution ───────────

if __name__ == "__main__":
    try:
        asyncio.run(collect_meteo_77(start_year=2020, end_year=2023))
    except KeyboardInterrupt:
        print("⏹️ Interrompu")
