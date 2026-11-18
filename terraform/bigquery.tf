resource "google_bigquery_dataset" "gold" {
  dataset_id = var.bq_dataset_id
  location = var.bq_location
  description = "Gold layer: validated weather observations."

  # Do not drop tables together with the dataset.
  delete_contents_on_destroy = false

  depends_on = [google_project_service.apis]
}

resource "google_bigquery_table" "weather_realtime" {
  dataset_id = google_bigquery_dataset.gold.dataset_id
  table_id = var.bq_table_id
  description = "Realtime weather observations loaded from silver CSV (append mode)."

  deletion_protection = true

  time_partitioning {
    type = "DAY"
    field = "event_time"
  }

  clustering = ["location_name"]

  # Column order must match the silver CSV header exactly.
  schema = jsonencode([
    { name = "source_object", type = "STRING", mode = "REQUIRED", description = "GCS path of the source bronze JSON file" },
    { name = "event_time", type = "TIMESTAMP", mode = "REQUIRED", description = "Value from data.time" },
    { name = "location_name", type = "STRING", mode = "REQUIRED", description = "location.name" },
    { name = "location_lat", type = "FLOAT", mode = "NULLABLE", description = "location.lat" },
    { name = "location_lon", type = "FLOAT", mode = "NULLABLE", description = "location.lon" },
    { name = "location_type", type = "STRING", mode = "NULLABLE", description = "location.type" },
    { name = "ingested_at_utc", type = "TIMESTAMP", mode = "REQUIRED", description = "When the row was processed by Airflow" },
    { name = "weather_temperature", type = "FLOAT", mode = "NULLABLE", description = "data.values.temperature" },
    { name = "weather_humidity", type = "FLOAT", mode = "NULLABLE", description = "data.values.humidity" },
    { name = "weather_windSpeed", type = "FLOAT", mode = "NULLABLE", description = "data.values.windSpeed" },
    { name = "weather_cloudCover", type = "FLOAT", mode = "NULLABLE", description = "data.values.cloudCover" },
    { name = "weather_precipitationProbability", type = "FLOAT", mode = "NULLABLE", description = "data.values.precipitationProbability" },
  ])
}
