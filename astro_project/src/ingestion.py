import requests
import json
import os
from datetime import datetime


def fetch_chicago_crime_data(
    base_url: str = "https://data.cityofchicago.org/resource/ijzp-q8t2.json",
    limit: int = 10000,
    max_pages: int = 100,
):
    print("Début de l'extraction API...")
    all_items = []

    for page in range(max_pages):
        offset = page * limit
        url = f"{base_url}?$limit={limit}&$offset={offset}"
        response = requests.get(url)
        data = response.json()
        if not data:
            break
        all_items.extend(data)

    # 1. On crée le chemin vers le dossier partagé 'include' d'Astro
    # (Le dossier 'include' existe par défaut à la racine de ton projet)
    save_dir = "/usr/local/airflow/include/data"
    os.makedirs(save_dir, exist_ok=True)

    # 2. On génère le nom du fichier
    file_name = f"raw_crimes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    file_path = os.path.join(save_dir, file_name)

    # 3. On sauvegarde "en dur"
    with open(file_path, "w") as f:
        json.dump(all_items, f, indent=2)

    print(f"Fichier sauvegardé avec succès dans : {file_path}")

    # 4. On renvoie uniquement LE CHEMIN pour que la tâche suivante (Soda ou Transform) le récupère
    return file_path
