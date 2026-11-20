import pandas as pd
from pathlib import Path

# Chemin du dataset
data_path = Path("data/raw/nasa_cmapps/train_FD001.txt")

# Noms des 26 colonnes
columns = (
    ["engine_id", "cycle"]
    + [f"setting_{i}" for i in range(1, 4)]
    + [f"sensor_{i}" for i in range(1, 22)]
)

# Lecture du fichier
df = pd.read_csv(
    data_path,
    sep=r"\s+",
    header=None,
    names=columns
)

# Affichage des informations
print("Dimensions :", df.shape)
print("\nPremières lignes :")
print(df.head())

print("\nNombre de moteurs :", df["engine_id"].nunique())
# Nombre de cycles par moteur
cycles_per_engine = df.groupby("engine_id")["cycle"].max()

print("\nNombre de cycles par moteur :")
print(cycles_per_engine)

# Statistiques des cycles
print("\nStatistiques des cycles :")
print(cycles_per_engine.describe())

# Vérification des valeurs manquantes
print("\nValeurs manquantes :")
print(df.isnull().sum().sum())