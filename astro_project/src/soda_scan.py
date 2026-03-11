from soda.scan import Scan


def run_soda_scan():
    print("--- DÉMARRAGE DU SCAN SODA SUR POSTGRES ---")

    scan = Scan()
    scan.set_data_source_name("postgres_chicago")

    # Chemins absolus vers tes fichiers YAML dans le conteneur Airflow
    scan.add_configuration_yaml_file(
        "/usr/local/airflow/include/soda/configuration.yml"
    )
    scan.add_sodacl_yaml_file("/usr/local/airflow/include/soda/checks.yml")

    # Exécution du scan
    exit_code = scan.execute()

    # Récupération des logs pour les afficher dans Airflow
    print(scan.get_logs_text())

    # Si des checks échouent, on fait planter la tâche Airflow pour alerter
    if exit_code != 0:
        raise ValueError(
            "❌ ALERTE SODA : Certaines règles de qualité ont échoué en base de données !"
        )

    print(
        "✅ SUCCÈS : Tous les checks Soda sont passés avec succès. La donnée est parfaite !"
    )
