data "local_file" "airports_schema" {
  filename = "${path.module}/airports_schema.json"
}

resource "google_bigquery_dataset" "dsongcp_dataset" {
  project    = var.project_id
  dataset_id = "dsongcp"
  location   = var.region
}

resource "google_bigquery_table" "dsongcp_airports" {
  dataset_id = google_bigquery_dataset.dsongcp_dataset.dataset_id
  table_id   = "airports"
  project    = var.project_id

  external_data_configuration {
    source_uris   = ["${var.ingestion_storage_bucket_uri}/airports/raw/airports.csv"]
    autodetect    = true
    source_format = "CSV"
    csv_options {
      skip_leading_rows = 1
      quote             = "\""
    }
  }

  schema = data.local_file.airports_schema.content
}

resource "google_bigquery_dataset_iam_member" "ingestion_service_account_bq_dataset" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.dsongcp_dataset.dataset_id
  role       = "roles/bigquery.dataOwner"
  member     = "serviceAccount:${var.ingestion_service_account_email}"
}

resource "google_project_iam_binding" "ingestion_service_account_bq_job" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  members = [
    "serviceAccount:${var.ingestion_service_account_email}"
  ]
}

resource "google_project_iam_binding" "ingestion_service_account_bq_edit" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  members = [
    "serviceAccount:${var.ingestion_service_account_email}"
  ]
}