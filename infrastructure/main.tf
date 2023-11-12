module "service-apis" {
  source     = "./service-apis"
  project_id = var.project_id
}

module "service-accounts" {
  source         = "./service-accounts"
  project_id     = var.project_id
  project_number = var.project_number
  depends_on     = [module.service-apis]
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
  region                          = var.region
  ingestion_service_account_email = module.service-accounts.ingestion_service_account_email
  ingestion_storage_bucket_uri    = module.storage-buckets.ingestion_storage_bucket_uri
  depends_on                      = [module.service-accounts]
}

module "pub-sub" {
  source = "./pub-sub"
  project_id = var.project_id
  region = var.region
  depends_on = [ module.service-apis, module.service-accounts ]
}