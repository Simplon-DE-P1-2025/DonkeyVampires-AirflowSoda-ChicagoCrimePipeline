from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.ingestion import fetch_chicago_crime_data
from src.soda_check import run_check_soda, run_check_soda_post_transform
from src.transformation import run_transform_chicago_crime
from src.filtering import run_filter_rows
from src.load import run_load


default_args = {
    "owner": "DonkeyVampires",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}
with DAG(
    'chicago_crime_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 3, 9),
    schedule='@daily',
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id='fetch_chicago_crime_data',
        python_callable=fetch_chicago_crime_data,
    )

    check_task = PythonOperator(
        task_id='check_soda',
        python_callable=run_check_soda,
    )

    transform_task = PythonOperator(
        task_id='transform_chicago_crime',
        python_callable=run_transform_chicago_crime,
    )

    check_post_transform_task = PythonOperator(
        task_id='check_soda_post_transform',
        python_callable=run_check_soda_post_transform,
    )

    filter_task = PythonOperator(
        task_id='filter_rows',
        python_callable=run_filter_rows,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=run_load,
    )

    ingest_task >> check_task >> transform_task >> check_post_transform_task >> filter_task >> load_task