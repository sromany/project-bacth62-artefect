#!/usr/bin/env python3
"""
Script de collecte de données météo par commune ou département.
"""

import asyncio
import argparse
import sys
from pathlib import Path
from loguru import logger

# ───────────── CONFIGURATION DU PYTHONPATH ─────────────
project_root = Path(__file__).resolve().parent.parent
src_path = project_root / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))

# ───────────── IMPORTS APRES AJOUT PYTHONPATH ─────────────
from data.collectors.meteo_france import MeteoFranceCollector, collect_meteo_data_for_communes
from config import settings

# ───────────── FONCTIONS DE COLLECTE ─────────────

async def collect_by_department(department_code: str, start_year: int, end_year: int):
    logger.info(f"🌤️ Collecte département {department_code} ({start_year}-{end_year})")
    async with MeteoFranceCollector() as collector:
        data = await collector.collect_department_data(department_code, start_year, end_year)

    if not data.empty:
        output_path = settings.RAW_DATA_PATH / "meteo" / "by_department"
        output_path.mkdir(parents=True, exist_ok=True)
        filename = f"meteo_{department_code}_{start_year}_{end_year}.parquet"
        data.to_parquet(output_path / filename)
        logger.info(f"✅ {len(data)} lignes sauvegardées dans {filename}")
    else:
        logger.warning("❌ Aucune donnée collectée")
    return data

async def collect_by_communes(commune_codes: list, start_year: int, end_year: int):
    logger.info(f"🌤️ Collecte {len(commune_codes)} communes ({start_year}-{end_year})")
    data = await collect_meteo_data_for_communes(commune_codes, start_year, end_year)

    if not data.empty:
        output_path = settings.RAW_DATA_PATH / "meteo" / "by_commune"
        output_path.mkdir(parents=True, exist_ok=True)
        filename = f"meteo_communes_{start_year}_{end_year}.parquet"
        data.to_parquet(output_path / filename)
        logger.info(f"✅ {len(data)} lignes sauvegardées dans {filename}")
    else:
        logger.warning("❌ Aucune donnée collectée")
    return data

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["department", "communes"], required=True)
    parser.add_argument("--department", type=str)
    parser.add_argument("--communes", nargs="+")
    parser.add_argument("--start-year", type=int, default=2020)
    parser.add_argument("--end-year", type=int, default=2023)
    parser.add_argument("--api-key", type=str)

    args = parser.parse_args()

    if args.api_key:
        settings.METEO_FRANCE_API_KEY = args.api_key

    logger.remove()
    logger.add(sys.stdout, format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")

    try:
        if args.mode == "department":
            if not args.department:
                parser.error("--department requis en mode department")
            result = asyncio.run(collect_by_department(args.department, args.start_year, args.end_year))
        else:
            if not args.communes:
                parser.error("--communes requis en mode communes")
            result = asyncio.run(collect_by_communes(args.communes, args.start_year, args.end_year))

        if result is not None:
            print("🎉 Collecte terminée avec succès !")
        else:
            sys.exit(1)
    except KeyboardInterrupt:
        print("⏹️ Collecte interrompue")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erreur inattendue : {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
