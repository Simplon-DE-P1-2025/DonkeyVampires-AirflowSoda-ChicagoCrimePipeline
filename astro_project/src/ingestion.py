import requests
import json
import os
from datetime import datetime
from typing import Optional, Dict, Any
import time

def fetch_chicago_crime_data(
    base_url: str = "https://data.cityofchicago.org/resource/ijzp-q8t2.json",
    params: Optional[Dict[str, Any]] = None,
    limit: int = 10000,      # nombre de lignes par page
    max_pages: int = 5,       # nombre de pages à récupérer
    max_retries: int = 3,     # retries en cas d'erreur
    request_delay: float = 1.0,
    write_json: bool = True
) -> str:
    """
    Récupère les données depuis l'API Chicago Crime pour un nombre limité de pages.
    """

    params = params or {}
    all_items = []

    for page in range(max_pages):
        offset = page * limit
        query_params = params.copy()
        query_params["$limit"] = limit
        query_params["$offset"] = offset

        for attempt in range(max_retries):
            try:
                response = requests.get(base_url, params=query_params, timeout=60)
                response.raise_for_status()
                items = response.json()
                break
            except requests.RequestException as e:
                print(f"Tentative {attempt+1}/{max_retries} pour page {page+1} échouée: {e}")
                if attempt == max_retries - 1:
                    raise
                time.sleep(request_delay)

        if not items:
            print(f"Aucune donnée à la page {page+1}, arrêt.")
            break

        all_items.extend(items)
        time.sleep(request_delay)

    # Création du dossier ingestion
    os.makedirs("data/ingestion", exist_ok=True)
    file_path = f"data/ingestion/chicago_crime_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    if write_json:
        with open(file_path, "w") as f:
            json.dump(all_items, f, indent=2)

    return file_path