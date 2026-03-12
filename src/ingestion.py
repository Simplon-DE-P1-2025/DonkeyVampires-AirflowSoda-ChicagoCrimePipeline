import requests
import json
import logging
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = (10, 120)
MAX_RETRIES = 5
BACKOFF_FACTOR = 1


def _build_session() -> requests.Session:
    """Session HTTP avec retry automatique sur timeouts et erreurs 5xx."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_chicago_crime_data(
    base_url: str = "https://data.cityofchicago.org/resource/ijzp-q8t2.json",
    limit: int = 20000,
    max_pages: int = 2,
):
    """
    Récupère les données de l'API Chicago Crime.

    Parameters
    ----------
    limit     : nombre d'enregistrements par page (défaut 10 000)
    max_pages : nombre max de pages à récupérer.
                Si None (défaut), pagine jusqu'à épuisement des données.
    """
    mode = f"max_pages={max_pages}" if max_pages is not None else "toutes les pages"
    logger.info("Début de l'extraction API — limit=%d, mode=%s", limit, mode)
    all_items = []
    session = _build_session()
    page = 0

    while True:
        if max_pages is not None and page >= max_pages:
            logger.info("Limite de %d pages atteinte — arrêt.", max_pages)
            break

        offset = page * limit
        url = f"{base_url}?$limit={limit}&$offset={offset}"
        logger.debug("Page %d — GET %s", page, url)

        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            logger.error(
                "Page %d — timeout après %ds (offset=%d). "
                "%d enregistrements déjà collectés seront sauvegardés.",
                page, REQUEST_TIMEOUT[1], offset, len(all_items),
            )
            break
        except requests.exceptions.RequestException as exc:
            logger.error("Page %d — erreur réseau : %s. Arrêt de la pagination.", page, exc)
            break

        data = response.json()
        if not data:
            logger.info("Page %d vide — fin de la pagination (total : %d enregistrements)", page, len(all_items))
            break

        all_items.extend(data)
        logger.info("Page %d — %d enregistrements récupérés (cumul : %d)", page, len(data), len(all_items))
        page += 1

    logger.info("Extraction terminée : %d enregistrements au total", len(all_items))

    # Chemin vers le dossier partagé 'include' d'Astro
    save_dir = "/usr/local/airflow/include/data/raw"
    os.makedirs(save_dir, exist_ok=True)

    file_path = os.path.join(save_dir, "raw_crimes.json")

    with open(file_path, "w") as f:
        json.dump(all_items, f, indent=2)

    logger.info("Fichier sauvegardé : %s (%d octets)", file_path, os.path.getsize(file_path))

    # Retourne uniquement le chemin pour la tâche suivante (XCom)
    return file_path
