import os
import logging
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

AIRFLOW_HOME = "/usr/local/airflow"

# Mêmes règles que soda_rules_secondcheck.yml — appliquées au niveau des lignes
VALID_PRIMARY_TYPES = {
    "THEFT", "BATTERY", "CRIMINAL DAMAGE", "MOTOR VEHICLE THEFT", "ASSAULT",
    "OTHER OFFENSE", "DECEPTIVE PRACTICE", "ROBBERY", "BURGLARY",
    "WEAPONS VIOLATION", "CRIMINAL TRESPASS", "NARCOTICS",
    "OFFENSE INVOLVING CHILDREN", "CRIM SEXUAL ASSAULT", "CRIMINAL SEXUAL ASSAULT",
    "SEX OFFENSE", "ARSON", "HOMICIDE", "INTERFERENCE WITH PUBLIC OFFICER",
    "GAMBLING", "STALKING", "INTIMIDATION", "KIDNAPPING",
    "LIQUOR LAW VIOLATION", "PUBLIC PEACE VIOLATION", "OBSCENITY",
    "NON-CRIMINAL", "PROSTITUTION", "HUMAN TRAFFICKING",
    "PUBLIC INDECENCY", "CONCEALED CARRY LICENSE VIOLATION",
}


def filter_rows(
    file_path: str,
    output_dir: str = os.path.join(AIRFLOW_HOME, "include/data/filtered"),
) -> dict:
    """
    Lit le fichier Parquet transformé et sépare les lignes valides des invalides.

    Une ligne est invalide si elle viole au moins une règle critique :
      - id manquant
      - date hors plage 2001–2026
      - year hors plage 2001–2026
      - latitude hors borne géographique de Chicago (si présente)
      - longitude hors borne géographique de Chicago (si présente)
      - primary_type absent ou non reconnu
      - district hors plage 1–25 (si présent)

    Retourne les chemins des deux fichiers (valid + invalid) via XCom.
    """
    df = pd.read_parquet(file_path)
    logger.info("Filtrage de %d lignes...", len(df))

    # Forcer les types numériques (sécurité si les colonnes sont stockées en str)
    for col in ("year", "district", "latitude", "longitude"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Construire un masque booléen : True = ligne valide
    mask = pd.Series(True, index=df.index)

    # id obligatoire
    mask &= df["id"].notna()

    # date dans la plage attendue
    if "date" in df.columns:
        mask &= df["date"].notna()
        mask &= df["date"].dt.year.between(2001, 2026)

    # year cohérent
    if "year" in df.columns:
        mask &= df["year"].notna()
        mask &= df["year"].between(2001, 2026)

    # primary_type reconnu
    if "primary_type" in df.columns:
        mask &= df["primary_type"].notna()
        mask &= df["primary_type"].isin(VALID_PRIMARY_TYPES)

    # district officiel Chicago (1–25), uniquement si la colonne est renseignée
    if "district" in df.columns:
        has_district = df["district"].notna()
        mask &= ~has_district | df["district"].between(1, 25)

    # coordonnées GPS (optionnelles mais validées si présentes)
    if "latitude" in df.columns:
        has_lat = df["latitude"].notna()
        mask &= ~has_lat | df["latitude"].between(41.60, 42.05)

    if "longitude" in df.columns:
        has_lon = df["longitude"].notna()
        mask &= ~has_lon | df["longitude"].between(-87.95, -87.50)

    df_valid   = df[mask]
    df_invalid = df[~mask]

    logger.info("Lignes valides : %d | Lignes invalides : %d", len(df_valid), len(df_invalid))

    # Sauvegarde
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    basename  = os.path.splitext(os.path.basename(file_path))[0]

    valid_path   = os.path.join(output_dir, f"valid_{basename}_{timestamp}.parquet")
    invalid_path = os.path.join(output_dir, f"invalid_{basename}_{timestamp}.parquet")

    df_valid.to_parquet(valid_path, index=False)
    df_invalid.to_parquet(invalid_path, index=False)

    logger.info("Valides   → %s", valid_path)
    logger.info("Invalides → %s", invalid_path)

    return {"valid": valid_path, "invalid": invalid_path}


def run_filter_rows(**context) -> dict:
    """Airflow-callable wrapper pour :func:`filter_rows`."""
    file_path = context["ti"].xcom_pull(task_ids="transform_chicago_crime")
    return filter_rows(file_path=file_path)
