# Only the secret "container" is managed here. The actual API key is added
# manually so it never appears in code or Terraform state:
#
#   echo -n "<API_KEY>" | gcloud secrets versions add tomorrow-io-api-key --data-file=-
resource "google_secret_manager_secret" "weather_api_key" {
  secret_id = var.weather_api_secret_id

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}
