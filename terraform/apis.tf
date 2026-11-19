locals {
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

resource "google_project_service" "apis" {
  for_each = local.required_apis

  project = var.project_id
  service = each.value

  # Never disable APIs on destroy: other resources/projects may rely on them.
  disable_on_destroy         = false
  disable_dependent_services = false
}
