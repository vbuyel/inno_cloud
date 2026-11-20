# Invoker permission: the scheduler SA may call ONLY this function

# A Gen2 function is served by a Cloud Run service with the same name;
# invocation is authorized by run.invoker on that service.
resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
  project  = var.project_id
  location = google_cloudfunctions2_function.ingest.location
  name     = google_cloudfunctions2_function.ingest.name
  role     = "roles/run.invoker"
  member   = google_service_account.scheduler.member
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
