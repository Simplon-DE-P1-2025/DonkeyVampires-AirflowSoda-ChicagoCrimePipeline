import pandas as pd
import glob
import os
from sqlalchemy import create_engine

# Connexion vers ton conteneur postgres_data (port interne 5432 car on est de Docker à Docker)
# On utilise 'host.docker.internal' car le postgres est sur un network different ou exposé sur l'hôte
DB_CONN = "postgresql://user:password@host.docker.internal:5433/chicago_data"


def load_to_postgres():
    # 1. Lister tous les fichiers JSON dans include/data
    path = "/usr/local/airflow/include/data"
    all_files = glob.glob(os.path.join(path, "*.json"))

    if not all_files:
        print("Aucun fichier JSON trouvé dans include/data/")
        return

    print(f"Fichiers trouvés : {all_files}")

    # 2. Lire et concaténer les données
    dfs = []
    for filename in all_files:
        df = pd.read_json(filename)
        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)

    # 3. Nettoyage de sécurité pour Postgres
    # On supprime la colonne 'location' qui est un dictionnaire (non supporté par un insert simple)
    if "location" in final_df.columns:
        final_df = final_df.drop(columns=["location"])

    # 4. Envoi vers Postgres
    print(f"Insertion de {len(final_df)} lignes dans Postgres...")
    engine = create_engine(DB_CONN)
    final_df.to_sql("final_crimes", engine, if_exists="replace", index=False)

    print("Chargement terminé avec succès dans la table 'final_crimes' !")
