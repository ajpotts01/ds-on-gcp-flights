resource "google_project_service" "cloud_run_service" {
  project = var.project_id
  service = "run.googleapis.com"
}

resource "google_project_service" "cloud_scheduler_service" {
  project = var.project_id
  service = "cloudscheduler.googleapis.com"
}

resource "google_project_service" "cloud_build_service" {
  project = var.project_id
  service = "cloudbuild.googleapis.com"
}

resource "google_project_service" "cloud_functions_service" {
  project = var.project_id
  service = "cloudfunctions.googleapis.com"
}

resource "google_project_service" "bigquery_datatransfer_service" {
  project = var.project_id
  service = "bigquerydatatransfer.googleapis.com"
}

resource "google_project_service" "gke_service" {
  project = var.project_id
  service = "container.googleapis.com"
}

resource "google_project_service" "dataflow_service" {
  project = var.project_id
  service = "dataflow.googleapis.com"
}

resource "google_project_service" "vertex_ai_service" {
  project = var.project_id
  service = "aiplatform.googleapis.com"
}