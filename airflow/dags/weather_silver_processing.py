"""weather_silver_processing: bronze JSON -> silver CSV -> gold BigQuery table.

Trigger: manual (no schedule, no catchup).
Params:
    source_path (optional) - "gs://bucket/prefix" or "prefix" in the data bucket.
                             If empty, the previous UTC hour of all locations is processed.

Flow:
    resolve_source -> list_bronze_files -> transform_to_silver -> load_to_gold

Configuration (Airflow Variables, set via AIRFLOW_VAR_* in docker-compose):
    weather_data_bucket, weather_bronze_prefix, weather_silver_prefix, weather_gold_table
"""

from __future__ import annotations

import logging
from dataclasses import asdict

import pendulum
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sdk import Param, Variable, dag, task
from airflow.sdk.exceptions import AirflowFailException

from include.transform import BronzeFile, NoValidRecordsError, SilverTransformer, TransformResult
from include.weather.partitioning import BronzeLayout, SilverLayout, SourceSelection
from include.weather.schema import BQ_SCHEMA

GCP_CONN_ID = "google_cloud_default"

logger = logging.getLogger(__name__)


def _bronze_layout() -> BronzeLayout:
    """Build the bronze layout from Airflow Variables.

    Single source of the bronze path convention for all tasks. Variable weather_bronze_prefix.

    Returns:
        BronzeLayout: Layout configured with the bronze prefix Variable.
    """
    return BronzeLayout(Variable.get("weather_bronze_prefix"))


def _download(hook: GCSHook, source: SourceSelection, objects: list[str]) -> list[BronzeFile]:
    """Download bronze files.

    Keep GCS I/O out of the pure transformation code.

    Args:
        hook: GCS hook.
        source: Bucket info.
        objects: Object names.

    Returns:
        list[BronzeFile]: Downloaded files with their raw bytes.
    """
    return [BronzeFile(source.bucket, name, hook.download(source.bucket, name)) for name in objects]


def _upload(hook: GCSHook, outputs: dict[str, str]) -> list[str]:
    """Upload silver CSV files to the data bucket.

    Persist the silver layer.

    Args:
        hook: GCS hook.
        outputs: {object_name: csv_text}.

    Returns:
        list[str]: Names of the uploaded silver objects.
    """
    bucket = Variable.get("weather_data_bucket")
    for object_name, csv_text in outputs.items():
        hook.upload(bucket, object_name, data=csv_text, mime_type="text/csv")
        logger.info("Wrote gs://%s/%s", bucket, object_name)
    return list(outputs)


def _transform(files: list[BronzeFile]) -> TransformResult:
    """Run the pure bronze -> silver transformation.

    Translate "no valid records" into a non-retryable DAG failure.

    Args:
        files: Downloaded bronze files.

    Returns:
        TransformResult: Silver CSV texts and run counters.

    Raises:
        AirflowFailException: If nothing is valid.
    """
    transformer = SilverTransformer(_bronze_layout(), SilverLayout(Variable.get("weather_silver_prefix")))
    try:
        return transformer.transform(files, ingested_at=pendulum.now("UTC"))
    except NoValidRecordsError as exc:
        raise AirflowFailException(str(exc)) from exc


@task
def resolve_source(params: dict, dag_run=None) -> dict:
    """Decide which bronze files to process.

    Default to the previous UTC hour relative to the run time, so retries of the same run read the
    same hour.

    Args:
        params: DAG params; ``source_path`` is optional.
        dag_run: The current DAG run, used as the time reference.

    Returns:
        dict: SourceSelection as dict {bucket, prefix, match_glob}.
    """
    bucket = Variable.get("weather_data_bucket")
    source_path = (params.get("source_path") or "").strip()
    if source_path:
        selection = _bronze_layout().from_source_path(source_path, default_bucket=bucket)
    else:
        reference = getattr(dag_run, "run_after", None) or pendulum.now("UTC")
        selection = _bronze_layout().previous_hour(bucket, reference)
    logger.info("Source: %s", selection.uri)
    return asdict(selection)


@task
def list_bronze_files(selection: dict) -> list[str]:
    """List JSON files of the selected source.

    Rule "input files exist": at least one .json must be present.

    Args:
        selection: Output of resolve_source.

    Returns:
        list[str]: Sorted names of the bronze .json objects.

    Raises:
        AirflowFailException: If no .json files are found.
    """
    source = SourceSelection(**selection)
    objects = GCSHook(gcp_conn_id=GCP_CONN_ID).list(
        bucket_name=source.bucket, prefix=source.prefix, match_glob=source.match_glob
    )
    objects = sorted(name for name in objects if name.endswith(".json"))
    if not objects:
        raise AirflowFailException(f"No .json files found in {source.uri}")
    logger.info("Found %d file(s) in %s", len(objects), source.uri)
    return objects


@task(multiple_outputs=True)
def transform_to_silver(selection: dict, objects: list[str]) -> dict:
    """Download, validate, flatten, deduplicate and write silver CSV files.

    Produce silver CSVs (one per location-hour) in the data bucket.

    Args:
        selection: Source bucket.
        objects: Bronze object names.

    Returns:
        dict: {"silver_objects": [...], valid/rejected/duplicates counters}.

    Raises:
        AirflowFailException: If no valid records remain after validation.
    """
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    result = _transform(_download(hook, SourceSelection(**selection), objects))
    silver_objects = _upload(hook, result.outputs)
    logger.info("Summary: %s", result.summary())
    return {"silver_objects": silver_objects, **result.summary()}


def load_to_gold(source_objects) -> GCSToBigQueryOperator:
    """Create the task that appends silver CSVs into the gold table.

    Load with the explicit schema; the table itself is owned by Terraform.

    Args:
        source_objects: XComArg with silver object names.

    Returns:
        GCSToBigQueryOperator: The ``load_to_gold`` task.
    """
    return GCSToBigQueryOperator(
        task_id="load_to_gold",
        gcp_conn_id=GCP_CONN_ID,
        bucket="{{ var.value.weather_data_bucket }}",
        source_objects=source_objects,
        destination_project_dataset_table="{{ var.value.weather_gold_table }}",
        schema_fields=list(BQ_SCHEMA),
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=False,
        create_disposition="CREATE_NEVER",
        write_disposition="WRITE_APPEND",
        max_bad_records=0,
    )


@dag(
    dag_id="weather_silver_processing",
    description="Bronze JSON (previous hour) -> validated silver CSV -> gold BigQuery table.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": pendulum.duration(minutes=1)},
    params={
        "source_path": Param(
            None,
            type=["null", "string"],
            description="Optional bronze prefix (gs://bucket/prefix or prefix). Empty = previous UTC hour.",
        ),
    },
    tags=["weather", "silver", "gold"],
)
def weather_silver_processing():
    """Wire the pipeline steps.

    Define task order; the logic lives in the module-level tasks.

    Returns:
        DAG: The weather_silver_processing DAG.
    """
    selection = resolve_source()
    silver = transform_to_silver(selection, list_bronze_files(selection))
    silver >> load_to_gold(silver["silver_objects"])


weather_silver_processing()
