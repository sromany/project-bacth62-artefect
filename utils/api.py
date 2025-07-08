import requests
import pandas as pd

def get_all_communes():
    url = "https://geo.api.gouv.fr/communes"
    params = {
        "fields": "nom,code,codeDepartement,population",
        "format": "json",
        "geometry": "centre"
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    return pd.DataFrame(response.json())