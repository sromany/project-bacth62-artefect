from pathlib import Path
import os

class Settings:
    RAW_DATA_PATH = Path("data/raw")
    METEO_FRANCE_API_KEY = os.getenv("METEO_FRANCE_API_KEY", "demo-key")

settings = Settings()