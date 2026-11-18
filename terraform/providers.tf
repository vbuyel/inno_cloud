provider "google" {
  project = var.project_id
  region  = var.region

  # Applied automatically to every resource that supports labels
  # (buckets, BigQuery datasets/tables, secrets, functions, ...).
  default_labels = local.common_labels
}
