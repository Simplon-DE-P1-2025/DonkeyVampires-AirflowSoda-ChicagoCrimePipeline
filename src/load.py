import os
import logging

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Charge le .env si présent (utile en local ; en prod Airflow utilise les Variables/Connections)
load_dotenv()

AIRFLOW_HOME = "/usr/local/airflow"


def get_engine() -> Engine:
    """Crée et retourne un moteur SQLAlchemy vers la base PostgreSQL."""
    host     = os.getenv("POSTGRES_HOST")
    port     = os.getenv("POSTGRES_PORT", "5432")
    db       = os.getenv("POSTGRES_DB")
    user     = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url, pool_pre_ping=True)
    logger.info("Connexion PostgreSQL : %s:%s/%s", host, port, db)
    return engine


def execute_sql_file(engine: Engine, sql_file_path: str) -> None:
    """Exécute toutes les instructions d'un fichier SQL."""
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    with engine.begin() as conn:
        conn.execute(text(sql))

    logger.info("Script SQL exécuté : %s", sql_file_path)


def execute_query(engine: Engine, query: str, params: dict = None) -> pd.DataFrame:
    """Exécute une requête SELECT et retourne le résultat en DataFrame."""
    with engine.connect() as conn:
        result = conn.execute(text(query), params or {})
        df = pd.DataFrame(result.fetchall(), columns=result.keys())
    logger.info("Query exécutée : %d lignes retournées", len(df))
    return df


def load_parquet(
    engine: Engine,
    parquet_path: str,
    table_name: str,
    if_exists: str = "append",
) -> int:
    """
    Charge un fichier Parquet dans une table PostgreSQL.

    Parameters
    ----------
    engine      : moteur SQLAlchemy
    parquet_path: chemin du fichier Parquet
    table_name  : nom de la table cible
    if_exists   : 'append' (défaut) | 'replace' | 'fail'

    Returns
    -------
    int : nombre de lignes chargées
    """
    df = pd.read_parquet(parquet_path)
    rows = len(df)

    df.to_sql(table_name, engine, if_exists=if_exists, index=False, method="multi", chunksize=1000)
    logger.info("Chargé %d lignes dans '%s'", rows, table_name)
    return rows


def run_load(**context) -> dict:
    """
    Airflow-callable wrapper.
    Charge uniquement les lignes valides dans PostgreSQL.
    Les lignes invalides restent en quarantaine au format Parquet.
    """
    filter_output = context["ti"].xcom_pull(task_ids="filter_rows")
    valid_path    = filter_output["valid"]
    invalid_path  = filter_output["invalid"]

    engine = get_engine()

    # Créer la table si elle n'existe pas encore
    sql_file = os.path.join(AIRFLOW_HOME, "include/create_tables.sql")
    execute_sql_file(engine, sql_file)

    # Charge uniquement les lignes valides en base
    valid_count = load_parquet(engine, valid_path, table_name="chicago_crime")

    # Les invalides restent en Parquet (quarantaine)
    logger.info("Quarantaine (Parquet) : %s", invalid_path)

    logger.info("Load terminé : %d lignes chargées en base", valid_count)
    return {"valid_rows_loaded": valid_count, "quarantine_path": invalid_path}
