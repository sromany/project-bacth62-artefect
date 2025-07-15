import toml
import os

def load_config():
    config_path = os.path.join("config", "app_config.toml")
    return toml.load(config_path)

# Accès direct
cfg = load_config()
PROJECT_ID = cfg["gcp"]["project_id"]
DATASET = cfg["gcp"]["dataset"]
TABLE = cfg["gcp"]["table"]
