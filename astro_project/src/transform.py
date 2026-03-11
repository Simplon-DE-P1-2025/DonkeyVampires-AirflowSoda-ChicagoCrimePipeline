import pandas as pd


def transform_data(ti=None):
    file_path = ti.xcom_pull(task_ids="check_raw_data")
    df = pd.read_json(file_path)

    print("--- DÉBUT DE LA TRANSFORMATION ---")

    # 1. Dédoublonnage de sécurité
    df_clean = df.drop_duplicates(subset=["id"]).copy()

    # 2. Enrichissement de la sélection des colonnes
    # On garde plus de choses intéressantes pour l'analyse
    cols_to_keep = [
        "id",
        "case_number",
        "date",
        "primary_type",
        "description",
        "location_description",
        "arrest",
        "domestic",
        "x_coordinate",
        "y_coordinate",
    ]

    # On s'assure de ne garder que les colonnes qui existent vraiment
    cols = [c for c in cols_to_keep if c in df_clean.columns]
    df_clean = df_clean[cols]

    # 3. Nettoyage des Textes & Valeurs par défaut
    if "location_description" in df_clean.columns:
        # Remplace les vides par 'UNKNOWN' et met tout en majuscules
        df_clean["location_description"] = (
            df_clean["location_description"].fillna("UNKNOWN").str.upper()
        )

    # 4. Standardisation des Dates (Format SQL classique)
    df_clean["date"] = pd.to_datetime(df_clean["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # 5. Filtrage Géographique (Basé sur ton YAML !)
    # On exclut les crimes qui ont des coordonnées aberrantes
    if "x_coordinate" in df_clean.columns and "y_coordinate" in df_clean.columns:
        # Convertir en numérique au cas où ce soit du texte
        df_clean["x_coordinate"] = pd.to_numeric(
            df_clean["x_coordinate"], errors="coerce"
        )
        df_clean["y_coordinate"] = pd.to_numeric(
            df_clean["y_coordinate"], errors="coerce"
        )

        # Application des limites de Chicago (prises de ton fichier soda_rules_firstcheck.yml)
        mask_x = df_clean["x_coordinate"].between(1050000, 1210000)
        mask_y = df_clean["y_coordinate"].between(1810000, 1955000)

        lignes_avant = len(df_clean)
        # On ne garde que les lignes qui respectent le masque (ou qui n'ont pas de coordonnées)
        df_clean = df_clean[(mask_x & mask_y) | df_clean["x_coordinate"].isnull()]
        lignes_filtrees = lignes_avant - len(df_clean)
        print(f"🧹 Filtrage géo : {lignes_filtrees} lignes aberrantes supprimées.")

    # Sauvegarde
    save_path = "/usr/local/airflow/include/data/clean_crimes.json"
    df_clean.to_json(save_path, orient="records", indent=2)

    print(f"✅ Transformation terminée. Lignes finales : {len(df_clean)}")
    return save_path
