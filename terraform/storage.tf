# Data lake bucket: bronze/ (raw JSON) and silver/ (CSV) prefixes.
resource "google_storage_bucket" "data" {
  name = local.data_bucket_name
  location = var.region

  storage_class = "STANDARD"
  uniform_bucket_level_access = true # required for IAM Conditions
  public_access_prevention = "enforced"
  force_destroy = false # never wipe data on `terraform destroy`

  versioning {
    enabled = true
  }

  dynamic "lifecycle_rule" {
    for_each = var.bronze_retention_days > 0 ? [1] : []
    content {
      condition {
        age = var.bronze_retention_days
        matches_prefix = [local.bronze_prefix]
      }
      action {
        type = "Delete"
      }
    }
  }

  # Clean up old noncurrent versions so versioning does not grow forever.
  lifecycle_rule {
    condition {
      days_since_noncurrent_time = 7
      with_state = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}

# Holds zipped Cloud Function source code (used in step 3).
resource "google_storage_bucket" "function_source" {
  name = local.source_bucket_name
  location = var.region

  uniform_bucket_level_access = true
  public_access_prevention = "enforced"
  force_destroy = false

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  depends_on = [google_project_service.apis]
}
