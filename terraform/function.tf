# Source package

data "archive_file" "function_src" {
  type        = "zip"
  source_dir  = "${path.module}/../function"
  output_path = "${path.module}/.build/weather-ingest.zip"
  excludes    = ["__pycache__", ".venv", "venv", ".env", "tests"]
}

# The object name contains the content hash, so any code change uploads a new
# object and forces a redeploy; unchanged code causes no diff.
resource "google_storage_bucket_object" "function_src" {
  bucket = google_storage_bucket.function_source.name
  name   = "weather-ingest/${data.archive_file.function_src.output_md5}.zip"
  source = data.archive_file.function_src.output_path
}

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
