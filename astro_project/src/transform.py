import pandas as pd
import os


def transform_data(ti=None):
    # On récupère le chemin du fichier brut via XCom
    file_path = ti.xcom_pull(task_ids="fetch_chicago_crime_data")
    if not file_path:
        raise ValueError("Aucun fichier brut trouvé via XCom")

    df = pd.read_json(file_path)

    # Transformation : on garde 5 colonnes clés et on filtre les arrestations
    cols = ["id", "case_number", "primary_type", "date", "arrest"]
    df_clean = df[cols].copy()

    # Sauvegarde
    save_path = "/usr/local/airflow/include/data/clean_crimes.json"
    df_clean.to_json(save_path, orient="records", indent=2)

    print(f"Transformation terminée : {save_path}")
    return save_path
