resource "google_storage_bucket" "flights_ingestion" {
  project  = var.project_id
  name     = "${var.project_id}-${var.bucket_name}"
  location = var.region

  uniform_bucket_level_access = false
  force_destroy               = "true"
  public_access_prevention    = "enforced"
}