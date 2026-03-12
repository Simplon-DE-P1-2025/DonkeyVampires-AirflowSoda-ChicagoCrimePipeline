"""Tests unitaires du DAG chicago_crime_pipeline."""

import pytest
from airflow.models import DagBag

DAG_ID = "chicago_crime_pipeline"
EXPECTED_TASKS = [
    "fetch_chicago_crime_data",
    "check_soda",
    "transform_chicago_crime",
    "check_soda_post_transform",
    "filter_rows",
    "load_to_postgres",
]


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


@pytest.fixture(scope="module")
def dag(dagbag):
    return dagbag.get_dag(DAG_ID)


# ─── DAG chargement ───────────────────────────────────────────────────────────

def test_dag_loaded(dagbag):
    """Le DAG est chargé sans erreur d'import."""
    assert DAG_ID in dagbag.dags, f"DAG '{DAG_ID}' introuvable"
    assert len(dagbag.import_errors) == 0, f"Erreurs d'import : {dagbag.import_errors}"


def test_dag_has_correct_number_of_tasks(dag):
    """Le DAG contient exactement 6 tâches."""
    assert len(dag.tasks) == len(EXPECTED_TASKS)


def test_dag_task_ids(dag):
    """Les IDs de tâches correspondent aux noms attendus."""
    task_ids = {task.task_id for task in dag.tasks}
    assert task_ids == set(EXPECTED_TASKS)


# ─── DAG configuration ────────────────────────────────────────────────────────

def test_dag_schedule(dag):
    """Le DAG est planifié en @daily."""
    assert dag.schedule == "@daily"


def test_dag_catchup_disabled(dag):
    """Le catchup est désactivé."""
    assert dag.catchup is False


def test_dag_owner(dag):
    """Le owner est DonkeyVampires."""
    assert dag.default_args.get("owner") == "DonkeyVampires"


def test_dag_retries(dag):
    """Les tâches ont 2 retries configurés."""
    assert dag.default_args.get("retries") == 2


# ─── Chaîne de dépendances ────────────────────────────────────────────────────

def test_dag_dependency_chain(dag):
    """Les tâches s'enchaînent dans le bon ordre."""
    chain = [
        ("fetch_chicago_crime_data", "check_soda"),
        ("check_soda", "transform_chicago_crime"),
        ("transform_chicago_crime", "check_soda_post_transform"),
        ("check_soda_post_transform", "filter_rows"),
        ("filter_rows", "load_to_postgres"),
    ]
    for upstream_id, downstream_id in chain:
        upstream = dag.get_task(upstream_id)
        downstream_ids = {t.task_id for t in upstream.downstream_list}
        assert downstream_id in downstream_ids, (
            f"'{downstream_id}' n'est pas en aval de '{upstream_id}'"
        )


def test_first_task_has_no_upstream(dag):
    """La première tâche n'a pas de dépendance amont."""
    first_task = dag.get_task("fetch_chicago_crime_data")
    assert len(first_task.upstream_list) == 0


def test_last_task_has_no_downstream(dag):
    """La dernière tâche n'a pas de dépendance aval."""
    last_task = dag.get_task("load_to_postgres")
    assert len(last_task.downstream_list) == 0
