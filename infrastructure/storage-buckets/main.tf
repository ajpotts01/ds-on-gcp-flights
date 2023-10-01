resource "google_storage_bucket" "flights_ingestion" {
  project  = var.project_id
  name     = "${var.project_id}-${var.bucket_name}"
  location = var.region

  uniform_bucket_level_access = true
  force_destroy               = "true"
  public_access_prevention    = "enforced"
}

resource "google_storage_bucket_iam_binding" "storage_bucket_binding" {
  bucket = google_storage_bucket.flights_ingestion.id
  role   = "roles/storage.admin"
  members = ["serviceAccount:${var.ingestion_service_account_email}"
  ]
}