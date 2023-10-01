resource "google_service_account" "ingestion_service_account" {
  account_id   = "ds-gcp-ingestion"
  project      = var.project_id
  display_name = "Ingestion Service Account"
}

resource "google_project_iam_member" "service_account_actor_binding" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

resource "google_project_iam_member" "cloud_run_invoker_binding" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

resource "google_project_iam_member" "cloud_scheduler_viewer_binding" {
  project = var.project_id
  role    = "roles/cloudscheduler.viewer"
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

resource "google_project_iam_member" "cloud_run_developer_binding" {
  project = var.project_id
  role    = "roles/run.developer"
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

resource "google_project_iam_member" "cloud_build_editor_binding" {
  project = var.project_id
  role    = "roles/cloudbuild.builds.editor"
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

output "ingestion_service_account_email" {
  value = google_service_account.ingestion_service_account.email
}