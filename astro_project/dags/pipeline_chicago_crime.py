from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from src.ingestion import fetch_chicago_crime_data

with DAG(
    'chicago_crime_pipeline',
    start_date=datetime(2026, 3, 9),
    schedule='@daily',
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='fetch_chicago_crime_data',
        python_callable=fetch_chicago_crime_data
    )