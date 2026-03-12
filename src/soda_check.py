import pandas as pd
import json
import os
import duckdb
from soda_duckdb import DuckDBDataSource
from soda_core.contracts import verify_contract_locally
from soda_core import configure_logging
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


AIRFLOW_HOME = "/usr/local/airflow"

def write_report(result, output_dir=os.path.join(AIRFLOW_HOME, "include/reports"), report_name="soda_report.md"):
    os.makedirs(output_dir, exist_ok=True)
    report_path = os.path.join(output_dir, report_name)

    md_lines = [
        "# Soda Data Quality Report",
        "",
        f"**Date** : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
    ]

    for contract_result in result.contract_verification_results:
        dataset_name = contract_result.contract.dataset_name
        status = contract_result.status

        logger.info(f"Dataset : {dataset_name} | Statut : {status}")

        started = getattr(contract_result, "started_timestamp", None)
        ended   = getattr(contract_result, "ended_timestamp", None)
        timing  = f"  |  **Début** : {started}  |  **Fin** : {ended}" if started else ""

        md_lines += [
            f"## Dataset : `{dataset_name}`",
            "",
            f"**Statut** : `{status}`{timing}",
            "",
        ]

        if contract_result.has_errors:
            errors = contract_result.get_errors_str()
            logger.error(f"Erreurs d'exécution : {errors}")
            md_lines += [f"> ❗ Erreurs d'exécution :\n> {errors}", ""]

        # Regrouper les check_results par colonne
        by_col = {}
        for cr in contract_result.check_results:
            col = cr.check.column_name or "Dataset"
            by_col.setdefault(col, []).append(cr)

        for col, check_results in by_col.items():
            md_lines += [
                f"### Colonne : `{col}`",
                "",
                "| | Check | Type | Statut | Seuil | Diagnostics |",
                "|--|-------|------|--------|-------|-------------|",
            ]
            for cr in check_results:
                emoticon    = cr.outcome_emoticon
                name        = cr.check.name or cr.check.type
                check_type  = cr.check.type
                outcome     = str(cr.outcome)
                threshold   = str(cr.threshold_value) if cr.threshold_value is not None else "-"
                diagnostics = cr.log_table_row_diagnostics(verbose=False) if cr.diagnostic_metric_values else "-"

                if cr.is_failed:
                    logger.error(f"  {emoticon} [{check_type}] {name} → {outcome} | seuil={threshold} | {diagnostics}")
                elif "WARNED" in outcome:
                    logger.warning(f"  {emoticon} [{check_type}] {name} → {outcome} | seuil={threshold}")
                else:
                    logger.info(f"  {emoticon} [{check_type}] {name} → {outcome}")

                md_lines.append(f"| {emoticon} | {name} | {check_type} | {outcome} | {threshold} | {diagnostics} |")

            md_lines.append("")

        md_lines += [
            "| Métrique | Valeur |",
            "|----------|--------|",
            f"| Total checks | {contract_result.number_of_checks} |",
            f"| ✅ Passés | {contract_result.number_of_checks_passed} |",
            f"| ❌ Échoués | {contract_result.number_of_checks_failed} |",
            "",
            "---",
            "",
        ]

    # Résumé global de la session
    logger.info(
        f"Session | total={result.number_of_checks} "
        f"passés={result.number_of_checks_passed} "
        f"échoués={result.number_of_checks_failed} "
        f"is_ok={result.is_ok}"
    )

    md_lines += [
        "## Résumé global de la session",
        "",
        "| Métrique | Valeur |",
        "|----------|--------|",
        f"| Total checks | {result.number_of_checks} |",
        f"| ✅ Passés | {result.number_of_checks_passed} |",
        f"| ❌ Échoués | {result.number_of_checks_failed} |",
        f"| is_passed | {result.is_passed} |",
        f"| is_failed | {result.is_failed} |",
        f"| is_warned | {result.is_warned} |",
        f"| has_errors | {result.has_errors} |",
        f"| is_ok | {result.is_ok} |",
    ]

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))

    logger.info(f"Rapport Markdown généré : {report_path}")


def check_soda(
    file_path: str,
    contract_file_path: str = os.path.join(AIRFLOW_HOME, "include/soda_scan/soda_rules_firstcheck.yml"),
    report_name: str = "soda_report.md",
):
    """
    Vérifie un fichier de données avec Soda selon les règles du contrat YAML.
    Supporte les formats JSON et Parquet (détection automatique par extension).
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Le fichier {file_path} est introuvable.")
    if not os.path.exists(contract_file_path):
        raise FileNotFoundError(f"Le fichier {contract_file_path} est introuvable.")

    # Chargement du fichier selon son format
    if file_path.endswith(".parquet"):
        df = pd.read_parquet(file_path)
    else:
        with open(file_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        # Ensure every item is a dict (filter out any stray non-dict entries)
        records = [r for r in raw if isinstance(r, dict)]
        df = pd.DataFrame(records)

    # Connexion DuckDB en mémoire
    conn = duckdb.connect(database=":memory:")
    cursor = conn.cursor()
    cursor.register("chicago_crime", df)

    duckdb_ds = DuckDBDataSource.from_existing_cursor(cursor, name="duckdb")
    configure_logging(verbose=False)

    result = verify_contract_locally(
        data_sources=[duckdb_ds],
        contract_file_path=contract_file_path,
    )

    soda_logs = result.get_logs_str()
    if soda_logs:
        logger.debug(f"Logs Soda internes :\n{soda_logs}")

    if result.has_errors:
        logger.error(f"Erreurs de vérification :\n{result.get_errors_str()}")
    elif result.is_failed:
        logger.warning(f"Contrat échoué : {result.number_of_checks_failed} check(s) en échec.")
    elif result.is_warned:
        logger.warning("Contrat avec avertissements.")
    else:
        logger.info(f"Contrat validé : {result.number_of_checks_passed}/{result.number_of_checks} checks passés.")

    write_report(result, report_name=report_name)
    return result


def _soda_result_to_dict(result) -> dict:
    """Sérialise le résultat Soda en dict JSON-compatible pour XCom Airflow."""
    return {
        "is_ok": result.is_ok,
        "number_of_checks": result.number_of_checks,
        "number_of_checks_passed": result.number_of_checks_passed,
        "number_of_checks_failed": result.number_of_checks_failed,
    }


def run_check_soda(**context) -> dict:
    """Check 1 — sur le fichier JSON brut issu de l'ingestion."""
    file_path = context["ti"].xcom_pull(task_ids="fetch_chicago_crime_data")
    logger.info("XCom file_path from ingestion: %s", file_path)
    contract = os.path.join(AIRFLOW_HOME, "include/soda_scan/soda_rules_firstcheck.yml")
    result = check_soda(file_path=file_path, contract_file_path=contract, report_name="soda_report_raw.md")
    return _soda_result_to_dict(result)


def run_check_soda_post_transform(**context) -> dict:
    """Check 2 — sur le fichier Parquet transformé."""
    file_path = context["ti"].xcom_pull(task_ids="transform_chicago_crime")
    logger.info("XCom file_path from transform: %s", file_path)
    contract = os.path.join(AIRFLOW_HOME, "include/soda_scan/soda_rules_secondcheck.yml")
    result = check_soda(file_path=file_path, contract_file_path=contract, report_name="soda_report_transformed.md")
    return _soda_result_to_dict(result)
