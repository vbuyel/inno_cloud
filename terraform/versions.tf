terraform {
  required_version = ">= 1.6.0, < 2.0.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.50"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }

  # State is local for now. For team / production use, move it to a GCS bucket
  # created out-of-band (bootstrap) and uncomment:
  #
  # backend "gcs" {
  #   bucket = "<project_id>-tfstate"
  #   prefix = "weather-pipeline"
  # }
}
