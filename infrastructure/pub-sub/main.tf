resource "google_pubsub_topic" "wheels_off" {
  project = var.project_id
  name    = "wheels-off"
}

resource "google_pubsub_topic" "arrived" {
  project = var.project_id
  name    = "arrived"
}

resource "google_pubsub_topic" "departed" {
  project = var.project_id
  name    = "departed"
}

