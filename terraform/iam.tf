# Service accounts — one per service

resource "google_service_account" "ingest" {
  account_id = "${local.name}-ingest"
  display_name = "Weather ingest Cloud Function runtime (${var.env})"
  depends_on = [google_project_service.apis]
}

resource "google_service_account" "scheduler" {
  account_id = "${local.name}-scheduler"
  display_name = "Cloud Scheduler invoker for weather ingest (${var.env})"
  depends_on = [google_project_service.apis]
}

resource "google_service_account" "airflow" {
  account_id = "${local.name}-airflow"
  display_name = "Local Airflow silver/gold processing (${var.env})"
  depends_on = [google_project_service.apis]
}

# Ingest function: write ONLY under bronze/, read ONLY the API key secret

resource "google_storage_bucket_iam_member" "ingest_bronze_writer" {
  bucket = google_storage_bucket.data.name
  role = "roles/storage.objectCreator"
  member = google_service_account.ingest.member

  condition {
    title = "bronze-prefix-only"
    description = "Ingest function may create objects only under bronze/"
    expression = "resource.name.startsWith(\"${local.data_bucket_objects}bronze/\")"
  }
}

resource "google_secret_manager_secret_iam_member" "ingest_secret_accessor" {
  secret_id = google_secret_manager_secret.weather_api_key.id
  role = "roles/secretmanager.secretAccessor"
  member = google_service_account.ingest.member
}

# Scheduler SA gets run.invoker on the function itself in step 3 (scheduler.tf),
# because that binding needs the function resource to exist.

# Airflow: read bronze, write silver, append into the gold table

# objects.list is evaluated on the bucket, so prefix conditions can't be used here.
resource "google_storage_bucket_iam_member" "airflow_reader" {
  bucket = google_storage_bucket.data.name
  role = "roles/storage.objectViewer"
  member = google_service_account.airflow.member
}

resource "google_storage_bucket_iam_member" "airflow_silver_writer" {
  bucket = google_storage_bucket.data.name
  role = "roles/storage.objectCreator"
  member = google_service_account.airflow.member

  condition {
    title = "silver-prefix-only"
    description = "Airflow may create objects only under silver/"
    expression  = "resource.name.startsWith(\"${local.data_bucket_objects}silver/\")"
  }
}

# Table-level (not dataset-level) write access.
resource "google_bigquery_table_iam_member" "airflow_gold_writer" {
  project = var.project_id
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id = google_bigquery_table.weather_realtime.table_id
  role = "roles/bigquery.dataEditor"
  member = google_service_account.airflow.member
}

# Load jobs can only be created at project level — no narrower scope exists.
resource "google_project_iam_member" "airflow_bq_job_user" {
  project = var.project_id
  role = "roles/bigquery.jobUser"
  member = google_service_account.airflow.member
}

# Lets listed humans run local Airflow as this SA without a JSON key.
resource "google_service_account_iam_member" "airflow_impersonation" {
  for_each = toset(var.airflow_impersonators)

  service_account_id = google_service_account.airflow.name
  role = "roles/iam.serviceAccountTokenCreator"
  member = each.value
}
