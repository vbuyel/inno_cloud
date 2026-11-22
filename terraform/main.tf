# Process A (ingestion), fully in the cloud:
#
#   Cloud Scheduler --(every 10 min, HTTP + OIDC)--> Cloud Function Gen2
#     1. reads the API key from Secret Manager          (secrets.tf)
#     2. GET tomorrow.io/realtime
#     3. writes the JSON as is to
#        gs://<data bucket>/bronze/weather/realtime/<loc>/YYYY/MM/DD/HH/<ts>.json
#                                                       (storage.tf)
#
# Supporting details live in separate files:
#   apis.tf            enabled GCP APIs
#   storage.tf         data bucket (bronze/, silver/) + function source bucket
#   function_source.tf zip of ../function uploaded to the source bucket
#   secrets.tf         tomorrow.io API key secret (container only)
#   iam.tf             service accounts and least-privilege bindings
#   bigquery.tf        gold dataset and table (used by local Airflow)

# Cloud Function Gen2 (runs on Cloud Run)

resource "google_cloudfunctions2_function" "ingest" {
  name        = "${local.name}-ingest"
  location    = var.region
  description = "Fetches tomorrow.io realtime weather and writes raw JSON to the bronze layer."

  build_config {
    runtime         = var.function_runtime
    entry_point     = "ingest"
    service_account = google_service_account.function_build.id

    source {
      storage_source {
        bucket = google_storage_bucket.function_source.name
        object = google_storage_bucket_object.function_src.name
      }
    }
  }

  service_config {
    service_account_email = google_service_account.ingest.email

    min_instance_count               = 0
    max_instance_count               = var.function_max_instances
    max_instance_request_concurrency = 1
    available_memory                 = "256M"
    timeout_seconds                  = 60

    # Endpoint is reachable from the internet (Cloud Scheduler needs that),
    # but every request must carry a valid identity token: no allUsers invoker.
    ingress_settings               = "ALLOW_ALL"
    all_traffic_on_latest_revision = true

    environment_variables = {
      DATA_BUCKET   = google_storage_bucket.data.name
      BRONZE_PREFIX = trimsuffix(local.bronze_prefix, "/")
      LOCATIONS     = join(";", [for l in var.weather_locations : trimspace(l)]) # ';' since "lat,lon" has commas
      WEATHER_UNITS = var.weather_units
    }

    # The key is read from Secret Manager at instance start; it never touches
    # Terraform code or state.
    secret_environment_variables {
      key        = "WEATHER_API_KEY"
      project_id = var.project_id
      secret     = google_secret_manager_secret.weather_api_key.secret_id
      version    = "latest"
    }
  }

  depends_on = [
    google_project_iam_member.function_build_roles,
    google_secret_manager_secret_iam_member.ingest_secret_accessor,
    google_storage_bucket_iam_member.ingest_bronze_writer,
  ]
}

# Cloud Scheduler job: trigger ingestion every 10 minutes

resource "google_cloud_scheduler_job" "ingest" {
  name        = "${local.name}-ingest-every-10m"
  region      = var.region
  description = "Triggers the weather ingest function (bronze layer)."

  schedule         = var.scheduler_cron
  time_zone        = "Etc/UTC"
  paused           = var.scheduler_paused
  attempt_deadline = "120s"

  # Non-2xx responses (e.g. 502 when nothing was stored) are retried once.
  # Partial success (207) is 2xx and is not retried.
  retry_config {
    retry_count          = 1
    min_backoff_duration = "30s"
    max_backoff_duration = "60s"
  }

  http_target {
    http_method = "POST"
    uri         = google_cloudfunctions2_function.ingest.url

    # Google-signed ID token for the scheduler SA; audience must be the function URL.
    oidc_token {
      service_account_email = google_service_account.scheduler.email
      audience              = google_cloudfunctions2_function.ingest.url
    }
  }

  depends_on = [google_cloud_run_v2_service_iam_member.scheduler_invoker]
}
