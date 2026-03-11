import pandas as pd
import os


def check_raw_data(ti=None):
    """Vérification des données brutes juste après l'ingestion API"""
    file_path = ti.xcom_pull(task_ids="fetch_chicago_crime_data")
    if not file_path or not os.path.exists(file_path):
        raise ValueError(f"Fichier brut introuvable : {file_path}")

    df = pd.read_json(file_path)
    print(f"--- ANALYSE DE QUALITÉ BRUTE (Total lignes : {len(df)}) ---")

    # 1. Check des Doublons (Assoupli en WARNING car c'est classique via API)
    if df["id"].duplicated().any():
        nb_doublons = df["id"].duplicated().sum()
        print(f"⚠️ WARNING : {nb_doublons} doublons d'ID détectés dans l'export brut.")
        print("Pas de problème, la tâche de transformation va les nettoyer.")
    else:
        print("✅ Doublons : OK (Aucun)")

    # 2. Check des Valeurs Manquantes Critiques
    critical_cols = ["case_number", "date", "primary_type", "description"]
    for col in critical_cols:
        if col in df.columns and df[col].isnull().any():
            print(f"❌ ÉCHEC : Valeurs nulles détectées dans la colonne '{col}'.")
            raise ValueError(f"Qualité compromise : Nulls dans {col}.")
    print("✅ Valeurs manquantes critiques : OK")

    # 3. Check Métier : Validité de l'Année (entre 2001 et 2026)
    if "year" in df.columns:
        years = df["year"].dropna().astype(int)
        if years.min() < 2001 or years.max() > 2026:
            print(
                "⚠️ WARNING : Des années hors de la plage [2001-2026] ont été détectées."
            )
        else:
            print("✅ Validité des années : OK")

    print("--- QUALITÉ BRUTE VALIDÉE ---")
    return file_path


def check_clean_data(ti=None):
    """Vérification des données nettoyées après la transformation"""
    file_path = ti.xcom_pull(task_ids="transform_data")

    if not file_path or not os.path.exists(file_path):
        raise ValueError(f"Fichier propre introuvable : {file_path}")

    df = pd.read_json(file_path)
    print(f"--- ANALYSE DE QUALITÉ PROPRE (Total lignes : {len(df)}) ---")

    # 1. Vérification des colonnes essentielles
    expected_columns = ["id", "case_number", "primary_type", "date", "arrest"]
    for col in expected_columns:
        if col not in df.columns:
            raise ValueError(
                f"❌ ERREUR : La colonne '{col}' a disparu pendant la transformation !"
            )
    print("✅ Structure des colonnes : OK")

    # 2. Vérification STRICTE des doublons (La transformation a dû faire son job)
    if df["id"].duplicated().any():
        raise ValueError(
            "❌ ERREUR : Il reste des doublons d'ID après la transformation ! La table finale ne sera pas mise à jour."
        )
    print("✅ Unicité des IDs : OK")

    # 3. Vérification des valeurs nulles sur l'ID
    if df["id"].isnull().any():
        raise ValueError("❌ ERREUR : Valeurs nulles détectées dans la colonne ID !")
    print("✅ Intégrité des IDs : OK")

    print(f"--- QUALITÉ PROPRE VALIDÉE ({len(df)} lignes prêtes pour Postgres) ---")
    return file_path
