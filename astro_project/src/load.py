import pandas as pd
from sqlalchemy import create_engine

# Connexion via le port exposé 5433
DB_CONN = "postgresql://user:password@host.docker.internal:5433/chicago_data"


def load_to_postgres(ti=None):
    # On récupère le chemin validé par le check de données propres
    file_path = ti.xcom_pull(task_ids="check_clean_data")

    if not file_path:
        raise ValueError("Aucun chemin de fichier reçu du check de qualité final")

    print(f"Chargement du fichier validé : {file_path}")
    df = pd.read_json(file_path)

    engine = create_engine(DB_CONN)
    df.to_sql("final_crimes", engine, if_exists="replace", index=False)

    print(f"Succès ! {len(df)} lignes insérées dans 'final_crimes'.")
