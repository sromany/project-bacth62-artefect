import toml
import os

# Config Streamlit
def load_config():
    config_path = os.path.join("config", "app_config.toml")
    return toml.load(config_path)

# Accès direct
cfg = load_config()
PROJECT_ID = cfg["gcp"]["project_id"]
DATASET = cfg["gcp"]["dataset"]
TABLE_TEMPERATURE = cfg["gcp"]["table_temperature"]
TABLE_CONSO = cfg["gcp"]["table_conso"]
TABLE_REG = cfg["gcp"]["table_reg"]
