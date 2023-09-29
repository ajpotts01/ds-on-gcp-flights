terraform {
  backend "gcs" {
    bucket = "ajp-ds-gcp-tf-state"
  }
}