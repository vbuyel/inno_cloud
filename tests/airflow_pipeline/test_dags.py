"""DAG integrity tests. Require Airflow: run inside the Airflow container (see README)."""

from pathlib import Path

import pytest

# "airflow.models", not "airflow": the project folder airflow/ is importable as a
# namespace package and would hide a missing Airflow installation.
pytest.importorskip("airflow.models", reason="Airflow is not installed (run these tests in the Airflow container)")

DAGS_DIR = Path(__file__).resolve().parents[2] / "airflow" / "dags"


@pytest.fixture(scope="module")
def dagbag():
    """Input: none. Purpose: parse all project DAGs once. Output: DagBag."""
    from airflow.models import DagBag

    return DagBag(dag_folder=str(DAGS_DIR))  # Airflow 3: examples are not loaded by DagBag


def test_no_import_errors(dagbag):
    """Every DAG file must import cleanly."""
    assert dagbag.import_errors == {}


def test_silver_dag_definition(dagbag):
    """Manual trigger, no catchup, optional source_path, expected task order."""
    dag = dagbag.get_dag("weather_silver_processing")
    assert type(dag.timetable).__name__ == "NullTimetable"  # schedule=None -> manual only
    assert dag.catchup is False
    assert dag.params["source_path"] is None
    assert dag.get_task("load_to_gold").upstream_task_ids == {"transform_to_silver"}
    assert dag.get_task("transform_to_silver").upstream_task_ids == {"resolve_source", "list_bronze_files"}


def test_load_appends_to_existing_table(dagbag):
    """Gold load: append mode, never creates the Terraform-owned table."""
    task = dagbag.get_dag("weather_silver_processing").get_task("load_to_gold")
    assert (task.write_disposition, task.create_disposition) == ("WRITE_APPEND", "CREATE_NEVER")
    assert task.autodetect is False and task.skip_leading_rows == 1
