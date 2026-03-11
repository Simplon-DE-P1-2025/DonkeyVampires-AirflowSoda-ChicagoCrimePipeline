from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Ajout du chemin src pour les imports
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from src.ingestion import fetch_chicago_crime_data
from src.data_quality import check_raw_data
from src.load import load_to_postgres  # <-- Nouvel import

with DAG(
    "chicago_crime_pipeline",
    start_date=datetime(2026, 3, 9),
    schedule="@daily",
    catchup=False,
) as dag:

    # Étape 1 : Ingestion
    ingest_task = PythonOperator(
        task_id="fetch_chicago_crime_data", python_callable=fetch_chicago_crime_data
    )

    # Étape 2 : Qualité (Soda)
    quality_check_task = PythonOperator(
        task_id="check_raw_data", python_callable=check_raw_data
    )

    # Étape 3 : Chargement final (Postgres)
    load_task = PythonOperator(
        task_id="load_to_postgres", python_callable=load_to_postgres
    )

    # Flux de données
    ingest_task >> quality_check_task >> load_task
