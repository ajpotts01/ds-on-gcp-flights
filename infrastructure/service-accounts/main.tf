resource "google_service_account" "ingestion_service_account" {
  account_id   = "ds-gcp-ingestion"
  project      = var.project_id
  display_name = "Ingestion Service Account"
}

resource "google_project_iam_member" "cloud_run_invoker_binding" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

output "ingestion_service_account_email" {
  value = google_service_account.ingestion_service_account.email
}