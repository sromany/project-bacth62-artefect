#!/usr/bin/env python3
"""
Affiche les données météo dans un tableau lisible
"""

import argparse
import pandas as pd
from rich.console import Console
from rich.table import Table
from pathlib import Path

def display_table(df: pd.DataFrame, max_rows: int = 20):
    """Affiche les premières lignes du DataFrame sous forme de tableau"""
    table = Table(show_header=True, header_style="bold cyan")

    for col in df.columns:
        table.add_column(str(col))

    for _, row in df.head(max_rows).iterrows():
        table.add_row(*(str(val) for val in row))

    console = Console()
    console.print(table)


def main():
    parser = argparse.ArgumentParser(description="Afficher un fichier parquet dans un tableau")
    parser.add_argument("--file", type=str, required=True, help="Chemin du fichier .parquet")
    parser.add_argument("--max-rows", type=int, default=20, help="Nombre de lignes à afficher")
    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        print(f"❌ Fichier introuvable : {file_path}")
        return

    df = pd.read_parquet(file_path)
    print(f"✅ Données chargées : {df.shape[0]} lignes × {df.shape[1]} colonnes\n")
    display_table(df, args.max_rows)


if __name__ == "__main__":
    main()
