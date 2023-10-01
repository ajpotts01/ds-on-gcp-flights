resource "google_storage_bucket" "cloud_build_logs" {
  project  = var.project_id
  name     = "${var.project_id}-cloud-build-logs"
  location = var.region

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
}

resource "google_storage_bucket" "cloud_build_storage" {
  project  = var.project_id
  name     = "${var.project_id}_cloudbuild"
  location = var.region

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
  retention_policy {
    retention_period = 86400
  }
}

resource "google_storage_bucket" "flights_ingestion" {
  project  = var.project_id
  name     = "${var.project_id}-${var.bucket_name}"
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = "true"
  public_access_prevention    = "enforced"
}

resource "google_storage_bucket_iam_binding" "build_logs_binding" {
  bucket = google_storage_bucket.cloud_build_logs.id
  role   = "roles/storage.admin"
  members = ["serviceAccount:${var.ingestion_service_account_email}"
  ]
}

resource "google_storage_bucket_iam_binding" "build_storage_binding" {
  bucket = google_storage_bucket.cloud_build_storage.id
  role   = "roles/storage.admin"
  members = [
    "serviceAccount:${var.ingestion_service_account_email}"
  ]
}

resource "google_storage_bucket_iam_binding" "storage_bucket_binding" {
  bucket = google_storage_bucket.flights_ingestion.id
  role   = "roles/storage.admin"
  members = ["serviceAccount:${var.ingestion_service_account_email}"
  ]
}