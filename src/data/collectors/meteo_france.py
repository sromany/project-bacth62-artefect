import pandas as pd
import aiohttp
import asyncio
from loguru import logger
from config import settings
import math
import random

class MeteoFranceCollector:
    def __init__(self):
        self.session = None
        self.api_key = settings.METEO_FRANCE_API_KEY

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.session.close()

    async def collect_department_data(self, department_code, start_year, end_year):
        # Exemple simulé
        logger.info(f"Simulation des données météo pour département {department_code}")
        return pd.DataFrame({
            "commune_code": ["75001", "75002"],
            "station_id": ["station1", "station2"],
            "year": [start_year, end_year],
            "temperature": [17.5, 18.1]
        })


# async def collect_meteo_data_for_communes(commune_codes, start_year, end_year):
#     logger.info("Simulation des données météo par communes")

#     rows = []
#     for commune_code in commune_codes:
#         base_offset = int(commune_code[-2:]) % 10  # décalage selon la commune

#         for year in range(start_year, end_year + 1):
#             for month in range(1, 13):
#                 # Température moyenne par mois, décalée pour avoir le pic en juillet/août
#                 seasonal_temp = 12 + 8 * math.sin((month - 1) / 12 * 2 * math.pi - math.pi / 2)

#                 # Ajout de l'offset commune + bruit aléatoire [-1, +1]
#                 temperature = seasonal_temp + base_offset * 0.3 + random.uniform(-1.5, 1.5)

#                 rows.append({
#                     "commune_code": commune_code,
#                     "station_id": "stationX",
#                     "year": year,
#                     "month": month,
#                     "temperature": round(temperature, 1),
#                     "date": f"{year}-{month:02d}-01"
#                 })

#     df = pd.DataFrame(rows)
#     df["date"] = pd.to_datetime(df["date"])

#     await asyncio.sleep(0)  # maintien de l'aspect async
#     return df



async def collect_meteo_data_for_communes(commune_codes, start_year, end_year):
    logger.info("📡 Simulation des données météo pour toutes les communes de France")

    data = []
    for code in commune_codes:
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                temp = round(random.uniform(-2, 30), 1)  # Simulation plus réaliste
                data.append({
                    "commune_code": code,
                    "month": f"{year}-{month:02d}",
                    "temperature": temp,
                })

    return pd.DataFrame(data)