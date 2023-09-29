# Potential service APIs
# gcp_service_list = [
#   "bigquery.googleapis.com",          # BigQuery - default on
#   "bigquerymigration.googleapis.com", # BigQuery - default on
#   "bigquerystorage.googleapis.com",   # BigQuery - default on
#   "cloudbuild.googleapis.com",        # Cloud Build - default on
#   "compute.googleapis.com",           # Compute Engine - default on
#   "containerregistry.googleapis.com", # Container Registry - default on
#   "deploymentmanager.googleapis.com", # Deployment Manager - default on
#   "iamcredentials.googleapis.com",    # IAM - default on
#   "logging.googleapis.com",           # Logging - default on
#   "notebooks.googleapis.com",         # Notebooks - default on
#   "oslogin.googleapis.com",           # OS Login - default on
#   "pubsub.googleapis.com",            # Pub/Sub - default on
#   "policyanalyzer.googleapis.com",    # Policy Analyzer - default on
#   "servicenetworking.googleapis.com", # Service Networking - default on
#   "serviceusage.googleapis.com",      # Service Usage - must be turned on or nothing else will work
#   "storage-api.googleapis.com"        # Storage JSON API - default on
# ]

# gcp_edg_service_list = [
#   "aiplatform.googleapis.com",           # Vertex - turned on by EDG
#   "artifactregistry.googleapis.com",     # Artifacts - turned on by EDG
#   "bigquerydatatransfer.googleapis.com", # BigQuery - turned on by EDG
#   "run.googleapis.com",                  # Cloud Run - turned on by EDG
#   "storage-component.googleapis.com",    # Storage - turned on by EDG
#   "cloudscheduler.googleapis.com",       # Cloud Scheduler - turned on by EDG
#   "cloudtasks.googleapis.com",           # Cloud Tasks - turned on by EDG
#   "cloudtrace.googleapis.com",           # Cloud Trace - turned on by EDG
#   "dataflow.googleapis.com"              # Dataflow - required for Vertex. Added to end to not mess up existing state
# ]

resource "google_project_service" "cloud_run_service" {
  project = var.project_id
  service = "run.googleapis.com"
}