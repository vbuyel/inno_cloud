"""Smoke-test DAG: verifies that local Airflow can reach GCP as the Airflow SA.

Run it manually from the UI. It does not modify anything.

Tasks:
    whoami       - logs which identity Google hooks use (must be the Airflow SA,
                   proving impersonation works and your own user is not used).
    list_bronze  - lists raw JSON files in the bronze layer.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.sdk import Variable, dag, task

GCP_CONN_ID = "google_cloud_default"
PREVIEW_LIMIT = 20

logger = logging.getLogger(__name__)


@task
def whoami() -> str:
    """Resolve the identity used by Google hooks.

    Proves that requests are made as the impersonated Airflow SA (connection
    ``google_cloud_default``) and not as your own user.

    Returns:
        str: The service account email, or the credentials class name if it has no email.

    Raises:
        google.auth.exceptions.RefreshError: If the mounted ADC is expired or the
            impersonation is not allowed.
    """
    credentials, project_id = GoogleBaseHook(gcp_conn_id=GCP_CONN_ID).get_credentials_and_project_id()
    identity = getattr(credentials, "service_account_email", None) or type(credentials).__name__
    logger.info("Project: %s | Acting as: %s", project_id, identity)
    return identity


@task
def list_bronze() -> int:
    """List raw JSON objects under the bronze prefix.

    Verifies read access to the data bucket and that ingestion produces files.
    Reads the Airflow Variables ``weather_data_bucket`` and ``weather_bronze_prefix``.

    Returns:
        int: The number of .json objects found (the newest ones are logged).
    """
    bucket = Variable.get("weather_data_bucket")
    prefix = Variable.get("weather_bronze_prefix").strip("/") + "/"
    objects = GCSHook(gcp_conn_id=GCP_CONN_ID).list(bucket_name=bucket, prefix=prefix, match_glob=f"{prefix}**.json")
    logger.info("Found %d JSON file(s) in gs://%s/%s", len(objects), bucket, prefix)
    for name in sorted(objects)[-PREVIEW_LIMIT:]:
        logger.info("  gs://%s/%s", bucket, name)
    if not objects:
        logger.warning("Bronze is empty: is the Cloud Scheduler job (step 4) applied and running?")
    return len(objects)


@dag(
    dag_id="gcp_connection_check",
    description="Checks GCP access (impersonation) and lists bronze files.",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["weather", "smoke-test"],
)
def gcp_connection_check():
    """Wire the smoke-test tasks.

    Confirms GCP connectivity before running the real pipeline.

    Returns:
        DAG: A DAG with the tasks ``whoami >> list_bronze``.
    """
    whoami() >> list_bronze()


gcp_connection_check()
