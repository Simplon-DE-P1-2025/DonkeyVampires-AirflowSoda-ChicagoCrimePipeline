import pandas as pd
import os


def check_raw_data(ti=None):
    file_path = ti.xcom_pull(task_ids="fetch_chicago_crime_data")
    if not file_path or not os.path.exists(file_path):
        raise ValueError(f"Fichier introuvable : {file_path}")

    df = pd.read_json(file_path)
    print(f"--- ANALYSE DE QUALITÉ (Total lignes : {len(df)}) ---")

    # 1. Check des Doublons (comme dans ton YAML)
    if df["id"].duplicated().any():
        print("❌ ÉCHEC : Des doublons d'ID ont été détectés.")
        raise ValueError("Qualité compromise : Doublons détectés.")
    else:
        print("✅ Doublons : OK")

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


# Tu peux garder ton check_clean_data existant en dessous
def check_clean_data(ti=None):
    # ... ton code actuel pour le clean check ...
    return ti.xcom_pull(task_ids="transform_data")
