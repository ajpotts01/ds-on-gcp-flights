resource "google_bigquery_dataset" "dsongcp_dataset" {
    project = var.project_id
    dataset_id = "dsongcp"
}

resource "google_bigquery_dataset_iam_member" "ingestion_service_account_bq_dataset" {
  project    = var.project_id
  dataset_id = "dsongcp"
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