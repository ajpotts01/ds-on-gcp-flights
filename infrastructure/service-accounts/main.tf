resource "google_service_account" "ingestion_service_account" {
  account_id   = "ds-gcp-ingestion"
  project      = var.project_id
  display_name = "Ingestion Service Account"
}

resource "google_service_account" "vertex_service_account" {
  account_id   = "ds-gcp-ml"
  project      = var.project_id
  display_name = "ML Service Account"
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
  role    = "roles/cloudscheduler.admin"
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

# Service account needs both Dataflow admin and Dataflow worker for some reason
# https://cloud.google.com/dataflow/docs/concepts/access-control#example
resource "google_project_iam_member" "dataflow_admin_binding" {
  project = var.project_id
  role    = "roles/dataflow.admin"
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

resource "google_project_iam_member" "dataflow_worker_binding" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

resource "google_project_iam_member" "compute_viewer_binding" {
  project = var.project_id
  role    = "roles/compute.viewer"
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

resource "google_project_iam_member" "storage_object_binding" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

resource "google_project_iam_member" "dataflow_compute_impersonate_token" {
  project = var.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:service-${var.project_number}@compute-system.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "dataflow_compute_impersonate_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:service-${var.project_number}@compute-system.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "dataflow_service_impersonate_token" {
  project = var.project_id
  role    = "roles/iam.serviceAccountTokenCreator"
  member  = "serviceAccount:service-${var.project_number}@dataflow-service-producer-prod.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "dataflow_service_impersonate_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:service-${var.project_number}@dataflow-service-producer-prod.iam.gserviceaccount.com"
}

resource "google_project_iam_member" "pubsub_admin" {
  project = var.project_id
  role    = "roles/pubsub.admin"
  member  = "serviceAccount:${google_service_account.ingestion_service_account.email}"
}

resource "google_project_iam_member" "vertex_service_agent" {
  project = var.project_id
  role    = "roles/aiplatform.serviceAgent"
  member  = "serviceAccount:${google_service_account.vertex_service_account.email}"
}

resource "google_project_iam_member" "vertex_notebook_service_agent" {
  project = var.project_id
  role    = "roles/notebooks.serviceAgent"
  member  = "serviceAccount:${google_service_account.vertex_service_account.email}"
}

resource "google_project_iam_member" "vertex_service_impersonate_user" {
  project = var.project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.vertex_service_account.email}"
}

output "ingestion_service_account_email" {
  value = google_service_account.ingestion_service_account.email
}

output "vertex_service_account_email" {
  value = google_service_account.vertex_service_account.email
}