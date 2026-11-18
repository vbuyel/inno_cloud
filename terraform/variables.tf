variable "project_id" {
  description = "GCP project ID where all resources are created."
  type = string

  validation {
    condition = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be 6-30 chars: lowercase letters, digits, hyphens; start with a letter."
  }
}

variable "region" {
  description = "Default GCP region for regional resources (function, scheduler, buckets)."
  type = string
  default = "us-central1"

  validation {
    condition = can(regex("^[a-z]+-[a-z]+[0-9]$", var.region))
    error_message = "region must look like a GCP region, e.g. us-central1 or europe-west1."
  }
}

variable "env" {
  description = "Environment name. Used in resource names and labels."
  type = string
  default = "dev"

  validation {
    condition = contains(["dev", "stage", "prod"], var.env)
    error_message = "env must be one of: dev, stage, prod."
  }
}

variable "owner" {
  description = "Owner label value (team or person handle, lowercase)."
  type = string

  validation {
    condition = can(regex("^[a-z][a-z0-9_-]{0,62}$", var.owner))
    error_message = "owner must be a valid GCP label value: lowercase letters, digits, '_' or '-', max 63 chars."
  }
}

variable "name_prefix" {
  description = "Short prefix for resource names, e.g. 'weather'."
  type = string
  default = "weather"

  validation {
    condition = can(regex("^[a-z][a-z0-9-]{1,14}$", var.name_prefix))
    error_message = "name_prefix must be 2-15 chars: lowercase letters, digits, hyphens; start with a letter."
  }
}

variable "bq_location" {
  description = "BigQuery dataset location. Should match bucket location to allow GCS -> BQ loads."
  type = string
  default = "US"
}

variable "bq_dataset_id" {
  description = "BigQuery dataset ID for the gold layer."
  type = string
  default = "weather_gold"

  validation {
    condition = can(regex("^[A-Za-z_][A-Za-z0-9_]*$", var.bq_dataset_id)) && length(var.bq_dataset_id) <= 1024
    error_message = "bq_dataset_id may contain only letters, digits and underscores."
  }
}

variable "bq_table_id" {
  description = "BigQuery table ID for the gold weather table."
  type = string
  default = "weather_realtime"

  validation {
    condition = can(regex("^[A-Za-z_][A-Za-z0-9_]*$", var.bq_table_id)) && length(var.bq_table_id) <= 1024
    error_message = "bq_table_id may contain only letters, digits and underscores."
  }
}

variable "weather_api_secret_id" {
  description = "Secret Manager secret ID that holds the tomorrow.io API key. The VALUE is added manually, never via Terraform."
  type = string
  default = "tomorrow-io-api-key"

  validation {
    condition = can(regex("^[A-Za-z0-9_-]{1,255}$", var.weather_api_secret_id))
    error_message = "weather_api_secret_id may contain only letters, digits, '_' and '-'."
  }
}

variable "bronze_retention_days" {
  description = "Delete raw bronze objects after this many days. 0 disables the rule."
  type = number
  default = 30

  validation {
    condition = var.bronze_retention_days >= 0 && var.bronze_retention_days <= 3650
    error_message = "bronze_retention_days must be between 0 and 3650."
  }
}

variable "airflow_impersonators" {
  description = "Principals allowed to impersonate the Airflow SA from a local machine, e.g. [\"user:me@example.com\"]."
  type = list(string)
  default = []

  validation {
    condition = alltrue([for m in var.airflow_impersonators : can(regex("^(user|group|serviceAccount):.+@.+$", m))])
    error_message = "Each member must be prefixed with user:, group: or serviceAccount:."
  }
}
