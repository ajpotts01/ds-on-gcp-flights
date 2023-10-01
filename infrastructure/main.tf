module "service-apis" {
  source     = "./service-apis"
  project_id = var.project_id
}

module "service-accounts" {
  source     = "./service-accounts"
  project_id = var.project_id
  depends_on = [module.service-apis]
}

module "storage-buckets" {
  source                          = "./storage-buckets"
  project_id                      = var.project_id
  region                          = var.region
  bucket_name                     = "flights"
  depends_on                      = [module.service-accounts]
  ingestion_service_account_email = module.service-accounts.ingestion_service_account_email
}

module "bigquery" {
  source                          = "./bigquery"
  project_id                      = var.project_id
  ingestion_service_account_email = module.service-accounts.ingestion_service_account_email
  depends_on                      = [module.service-accounts]
}