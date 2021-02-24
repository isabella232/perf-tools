provider "google" {
  credentials = file(var.credentials_path)
  project     = var.project_id
  region      = var.default_region
  zone        = var.default_zone
}

provider "google-beta" {
  credentials = file(var.credentials_path)
  project     = var.project_id
  region      = var.default_region
  zone        = var.default_zone
}
