#!/usr/bin/env python3
"""
Liste toutes les communes présentes dans un fichier météo .parquet.
"""

import argparse
import pandas as pd
from pathlib import Path

def list_communes(file_path: Path):
    """Charge un fichier Parquet et retourne la liste des communes uniques"""
    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(f"❌ Erreur lors de la lecture du fichier : {e}")
        return

    # Détection des colonnes disponibles
    if "commune_code" not in df.columns:
        print("❌ La colonne 'commune_code' est absente du fichier.")
        return

    has_name = "commune_name" in df.columns
    grouped = df[["commune_code"] + (["commune_name"] if has_name else [])].drop_duplicates().sort_values("commune_code")

    print(f"🏘️ {len(grouped)} communes trouvées :\n")
    for _, row in grouped.iterrows():
        code = row["commune_code"]
        if has_name:
            name = row["commune_name"]
            print(f"- {code} : {name}")
        else:
            print(f"- {code}")


def main():
    parser = argparse.ArgumentParser(description="Liste les communes contenues dans un fichier météo .parquet")
    parser.add_argument("--file", type=str, required=True, help="Chemin du fichier .parquet à analyser")

    args = parser.parse_args()
    file_path = Path(args.file)

    if not file_path.exists():
        print(f"❌ Fichier introuvable : {file_path}")
        return

    list_communes(file_path)


if __name__ == "__main__":
    main()
