import json
import time
from kafka import KafkaProducer
from pathlib import Path

# 1. Chemin du dataset NASA C-MAPSS
DATA_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "raw"
    / "nasa_cmapps"
    / "train_FD001.txt"
)

# 2. Configuration Kafka et rejeu en flux
TOPIC_NAME = "capteurs_moteurs"
REPLAY_DELAY = 0.5

# 3. Noms des 26 colonnes
COLUMNS = (
    ["engine_id", "cycle"]
    + [f"setting_{i}" for i in range(1, 4)]
    + [f"sensor_{i}" for i in range(1, 22)]
)

# 4. Connexion au broker Kafka
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda data: json.dumps(data).encode("utf-8")
)

# 5. Lecture et rejeu progressif des observations
sent_count = 0

try:
    with DATA_PATH.open("r") as file:
        for line in file:

            values = line.split()

            if len(values) != 26:
                raise ValueError("Ligne invalide : 26 valeurs attendues")

            # Conversion de la ligne en dictionnaire
            record = dict(zip(COLUMNS, values))

            record["engine_id"] = int(record["engine_id"])
            record["cycle"] = int(record["cycle"])

            for column in COLUMNS[2:]:
                record[column] = float(record[column])

            # Envoi du JSON complet vers Kafka
            producer.send(TOPIC_NAME, value=record)
            producer.flush()

            sent_count += 1

            # Affichage du JSON complet dans PowerShell
            print(f"\nObservation {sent_count} envoyée :")
            print(json.dumps(record, indent=4), flush=True)

            # Attente de 0.5 seconde avant l'observation suivante
            time.sleep(REPLAY_DELAY)

    print("\nRejeu terminé avec succès !")
    print("Nombre total de messages envoyés :", sent_count)

except Exception as error:
    print("Erreur pendant le rejeu :", error)

finally:
    producer.close()
