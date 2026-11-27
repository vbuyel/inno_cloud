locals {
  name = "${var.name_prefix}-${var.env}"

  common_labels = {
    env        = var.env
    owner      = var.owner
    project    = "weather-pipeline"
    managed_by = "terraform"
  }

  # Bucket names are global, so the project ID is included to avoid collisions.
  data_bucket_name   = "${var.project_id}-${local.name}-data"
  source_bucket_name = "${var.project_id}-${local.name}-fn-src"

  bronze_prefix = "bronze/weather/realtime/"
  silver_prefix = "silver/weather/realtime/"

  # Used in IAM Conditions to scope object-level permissions to a prefix.
  data_bucket_objects = "projects/_/buckets/${local.data_bucket_name}/objects/"

  # GCP APIs enabled in apis.tf.
  required_apis = toset([
    "iam.googleapis.com",
    "iamcredentials.googleapis.com", # SA impersonation for local Airflow
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "secretmanager.googleapis.com",
    # Needed in later steps (function + scheduler); enabling early avoids propagation delays.
    "cloudfunctions.googleapis.com",
    "run.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "cloudscheduler.googleapis.com",
    "logging.googleapis.com",
  ])
}
