output "data_bucket" {
  description = "Data lake bucket name (bronze/ and silver/ prefixes)."
  value       = google_storage_bucket.data.name
}

output "bronze_uri" {
  description = "Base URI for raw JSON files."
  value       = "gs://${google_storage_bucket.data.name}/${local.bronze_prefix}"
}

output "silver_uri" {
  description = "Base URI for processed CSV files."
  value       = "gs://${google_storage_bucket.data.name}/${local.silver_prefix}"
}

output "function_source_bucket" {
  description = "Bucket holding zipped Cloud Function source."
  value       = google_storage_bucket.function_source.name
}

output "gold_table" {
  description = "Fully qualified BigQuery gold table ID."
  value       = "${var.project_id}.${google_bigquery_dataset.gold.dataset_id}.${google_bigquery_table.weather_realtime.table_id}"
}

output "weather_api_secret" {
  description = "Secret Manager secret resource name for the tomorrow.io API key."
  value       = google_secret_manager_secret.weather_api_key.id
}

output "service_accounts" {
  description = "Service account emails per service."
  value = {
    ingest         = google_service_account.ingest.email
    scheduler      = google_service_account.scheduler.email
    airflow        = google_service_account.airflow.email
    function_build = google_service_account.function_build.email
  }
}

output "function_name" {
  description = "Cloud Function Gen2 name."
  value       = google_cloudfunctions2_function.ingest.name
}

output "function_url" {
  description = "HTTPS endpoint of the ingest function (requires an identity token)."
  value       = google_cloudfunctions2_function.ingest.url
}

output "scheduler_job" {
  description = "Cloud Scheduler job that triggers the ingest function."
  value       = google_cloud_scheduler_job.ingest.name
}

output "scheduler_cron" {
  description = "Effective trigger schedule (UTC)."
  value       = google_cloud_scheduler_job.ingest.schedule
}

output "function_run_service" {
  description = "Underlying Cloud Run service name (target for run.invoker bindings)."
  value       = google_cloudfunctions2_function.ingest.service_config[0].service
}
