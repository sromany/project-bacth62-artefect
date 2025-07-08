import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

# ─── Ajout du chemin racine du projet pour les imports ──────────
project_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(project_root))

# ─── Imports internes et externes ────────────────────────────────
from utils.api import get_all_communes
from data.collectors.meteo_france import collect_meteo_data_for_communes
from loguru import logger
import pandas as pd
import asyncio

# ─── Fonction principale ─────────────────────────────────────────
async def collect_meteo_france(start_year=2020, end_year=2023):
    df_communes = get_all_communes()
    commune_codes = df_communes["code"].tolist()
    #commune_codes = commune_codes[:2000] 

    logger.info(f"📍 {len(commune_codes)} communes à traiter dans toute la France")

    df = await collect_meteo_data_for_communes(commune_codes, start_year, end_year)

    if df is None or df.empty:
        logger.warning("❌ Aucune donnée météo récupérée")
        return

    df.to_parquet("data/temperature_mensuelle_france_2020_2023.parquet", index=False)
    logger.success("✅ Températures mensuelles sauvegardées dans data/temperature_mensuelle_france_2020_2023.parquet")

# ─── Lancement du script ─────────────────────────────────────────
if __name__ == "__main__":
    asyncio.run(collect_meteo_france())
