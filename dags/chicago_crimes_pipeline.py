from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta  # ← ici
import requests
import pandas as pd
import subprocess

# ─── Configuration ───────────────────────────────────────
API_URL = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
LIMIT = 1000000
CONN_ID = "postgres_dataops"

default_args = {
    "owner": "dataops_team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

# ─── Tâche 1 : Ingestion ─────────────────────────────────
def ingest_data(**context):
    print(f"Ingestion depuis {API_URL}")
    response = requests.get(API_URL, params={"$limit": LIMIT}, timeout=60)
    response.raise_for_status()
    
    data = response.json()
    print(f"✅ {len(data)} enregistrements récupérés")
    
    # Pousser les données dans XCom
    context["ti"].xcom_push(key="raw_count", value=len(data))
    return data  # Airflow sérialise en XCom automatiquement

# ─── Tâche 2 : Charger données brutes en PostgreSQL ──────
def load_raw_data(**context):
    data = context["ti"].xcom_pull(task_ids="ingest_data")
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Créer la table
    with open("/usr/local/airflow/include/sql/create_tables.sql") as f:
        cursor.execute(f.read())

    # Truncate avant insertion
    cursor.execute("TRUNCATE TABLE raw_crimes")

    # Insérer les données
    insert_query = """
        INSERT INTO raw_crimes 
        (id, date, block, primary_type, description, location_description,
         arrest, domestic, district, ward, community_area, latitude, longitude, year)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
    """
    
    records = []
    for row in data:
        records.append((
            row.get("id"),
            row.get("date"),
            row.get("block"),
            row.get("primary_type"),
            row.get("description"),
            row.get("location_description"),
            row.get("arrest") == "true" if isinstance(row.get("arrest"), str) else bool(row.get("arrest")),
            row.get("domestic") == "true" if isinstance(row.get("domestic"), str) else bool(row.get("domestic")),
            row.get("district"),
            row.get("ward"),
            row.get("community_area"),
            float(row["latitude"]) if row.get("latitude") else None,
            float(row["longitude"]) if row.get("longitude") else None,
            row.get("year"),
        ))
    
    cursor.executemany(insert_query, records)
    conn.commit()
    cursor.close()
    print(f"✅ {len(records)} lignes insérées dans raw_crimes")

# ─── Tâche 3 : Soda Check #1 (données brutes) ────────────
def soda_check_raw():
    result = subprocess.run(
        ["soda", "scan", "-d", "postgres", "-c", 
         "/usr/local/airflow/include/soda/configuration.yml",
         "/usr/local/airflow/include/soda/checks_raw.yml"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise ValueError("❌ Soda Check #1 échoué — données brutes non conformes")
    print("✅ Soda Check #1 passé")

# ─── Tâche 4 : Transformation ────────────────────────────
def transform_data():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    
    # Lire les données brutes
    df = hook.get_pandas_df("SELECT * FROM raw_crimes")
    print(f"📊 {len(df)} lignes à transformer")
    
    # Filtrer les lignes sans type de crime ou sans année
    df = df.dropna(subset=["primary_type", "year"])
    
    # Agrégation par type de crime, année, district
    agg = df.groupby(["primary_type", "year", "district"]).agg(
        total_crimes=("id", "count"),
        arrest_count=("arrest", "sum")
    ).reset_index()
    
    # Calcul du taux d'arrestation
    agg["arrest_rate"] = agg["arrest_count"] / agg["total_crimes"]
    
    # Charger dans la table transformée
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE transformed_crimes")
    
    for _, row in agg.iterrows():
        cursor.execute("""
            INSERT INTO transformed_crimes 
            (primary_type, year, district, total_crimes, arrest_count, arrest_rate)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (row["primary_type"], row["year"], row["district"],
              int(row["total_crimes"]), int(row["arrest_count"]),
              float(row["arrest_rate"])))
    
    conn.commit()
    cursor.close()
    print(f"✅ {len(agg)} lignes insérées dans transformed_crimes")

# ─── Tâche 5 : Soda Check #2 (données transformées) ──────
def soda_check_transformed():
    result = subprocess.run(
        ["soda", "scan", "-d", "postgres", "-c",
         "/usr/local/airflow/include/soda/configuration.yml",
         "/usr/local/airflow/include/soda/checks_transformed.yml"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(result.stderr)
        raise ValueError("❌ Soda Check #2 échoué — données transformées non conformes")
    print("✅ Soda Check #2 passé")

# ─── Définition du DAG ───────────────────────────────────
with DAG(
    dag_id="chicago_crimes_pipeline",
    default_args=default_args,
    description="Pipeline DataOps - Chicago Crimes",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),  # ← ici
    catchup=False,
    tags=["dataops", "crimes", "chicago"],
) as dag:

    t1_ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=ingest_data,
    )

    t2_load_raw = PythonOperator(
        task_id="load_raw_data",
        python_callable=load_raw_data,
    )

    t3_soda_raw = PythonOperator(
        task_id="soda_check_raw",
        python_callable=soda_check_raw,
    )

    t4_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    t5_soda_transformed = PythonOperator(
        task_id="soda_check_transformed",
        python_callable=soda_check_transformed,
    )

    # Dépendances
    t1_ingest >> t2_load_raw >> t3_soda_raw >> t4_transform >> t5_soda_transformed