# resource "google_notebooks_instance" "vertex_notebook" {
#   name         = "${var.project_id}-eda"
#   location     = "${var.region}-a"
#   machine_type = "n1-standard-4"

#   service_account = var.vertex_service_account_email

#   boot_disk_type    = "PD_STANDARD"
#   boot_disk_size_gb = "100"
# }

# This should create a Google managed notebook?
resource "google_notebooks_runtime" "vertex_notebook_eda" {
  project  = var.project_id
  name     = "${var.project_id}-eda"
  location = var.region

  access_config {
    access_type   = "SERVICE_ACCOUNT"
    runtime_owner = var.vertex_service_account_email
  }

  virtual_machine {
    virtual_machine_config {
      machine_type = "n1-standard-4"
      data_disk {
        initialize_params {
          disk_size_gb = 100
          disk_type    = "PD_STANDARD"
        }
      }
    }
  }
}