import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Chemin du fichier
project_root = Path(__file__).resolve().parents[1]
df = pd.read_parquet(project_root / "data" / "temperature_mensuelle_77_2020_2023.parquet")

# Conversion en timestamp
df["month"] = df["month"].dt.to_timestamp()

# Filtrage sur une commune (ex : 77001)
df_commune = df[df["commune_code"] == "77001"]

# Vérif : données présentes ?
if df_commune.empty:
    print("⚠️ Aucune donnée pour la commune 77001")
else:
    print(f"✅ {len(df_commune)} lignes pour la commune 77001")
    # Tracé
    plt.figure(figsize=(12, 6))
    plt.plot(df_commune["month"], df_commune["temperature"], marker="o")
    plt.title("Température mensuelle - Commune 77001")
    plt.xlabel("Mois")
    plt.ylabel("Température (°C)")
    plt.grid(True)
    plt.tight_layout()
    # Enregistrement
    output_path = project_root / "outputs" / "plot_temp_77001.png"
    output_path.parent.mkdir(exist_ok=True)
    plt.savefig(output_path)
    print(f"✅ Graphique sauvegardé dans {output_path}")
