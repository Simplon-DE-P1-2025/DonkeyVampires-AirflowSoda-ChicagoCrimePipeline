import pandas as pd
import os


def check_raw_data(ti=None):
    """Vérification des données brutes après l'ingestion"""
    file_path = ti.xcom_pull(task_ids="fetch_chicago_crime_data")

    if not file_path or not os.path.exists(file_path):
        raise ValueError(f"Fichier brut introuvable : {file_path}")

    df = pd.read_json(file_path)
    row_count = len(df)

    print(f"--- CHECK DONNÉES BRUTES ---")
    print(f"Lignes trouvées : {row_count}")

    if row_count == 0:
        raise ValueError("ÉCHEC : Le fichier brut est vide !")

    print("Qualité brute validée.")
    return file_path


def check_clean_data(ti=None):
    """Vérification des données nettoyées après transformation"""
    file_path = ti.xcom_pull(task_ids="transform_data")

    if not file_path or not os.path.exists(file_path):
        raise ValueError(f"Fichier propre introuvable : {file_path}")

    df = pd.read_json(file_path)

    # On vérifie que les colonnes essentielles sont là
    expected_columns = ["id", "case_number", "primary_type", "date", "arrest"]

    print(f"--- CHECK DONNÉES PROPRES ---")
    for col in expected_columns:
        if col not in df.columns:
            raise ValueError(
                f"ERREUR : La colonne '{col}' manque après transformation !"
            )

    print(f"Qualité propre validée ({len(df)} lignes conformes).")
    return file_path
