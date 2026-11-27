# Source package of the ingest Cloud Function (deployed in main.tf).

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
