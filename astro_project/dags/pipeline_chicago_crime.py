from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Ajout du chemin pour les imports
sys.path.append("/usr/local/airflow")

from src.ingestion import fetch_chicago_crime_data
from src.data_quality import check_raw_data, check_clean_data  # On importe les deux
from src.transform import transform_data
from src.load import load_to_postgres

with DAG(
    "chicago_crime_pipeline",
    start_date=datetime(2026, 3, 9),
    schedule="@daily",
    catchup=False,
) as dag:

    # 1. Ingestion
    ingest_t = PythonOperator(
        task_id="fetch_chicago_crime_data", python_callable=fetch_chicago_crime_data
    )

    # 2. Qualité sur le brut
    quality_raw_t = PythonOperator(
        task_id="check_raw_data", python_callable=check_raw_data
    )

    # 3. Transformation
    transform_t = PythonOperator(
        task_id="transform_data", python_callable=transform_data
    )

    # 4. Qualité sur le propre (Nouvelle étape)
    quality_clean_t = PythonOperator(
        task_id="check_clean_data", python_callable=check_clean_data
    )

    # 5. Chargement final
    load_t = PythonOperator(
        task_id="load_to_postgres", python_callable=load_to_postgres
    )

    # L'ordre logique du flux
    ingest_t >> quality_raw_t >> transform_t >> quality_clean_t >> load_t
