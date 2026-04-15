provider "google" {
  credentials = file("${path.module}/../keys/my-creds.json")
  project     = var.project_id
  region      = var.region
}

resource "google_storage_bucket" "raw_data_lake" {
  name                        = var.bucket_name
  location                    = var.region
  force_destroy               = true
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }

  labels = {
    project = "uk-flight-punctuality"
  }
}

resource "google_bigquery_dataset" "flight_data" {
  dataset_id = var.dataset_id
  project    = var.project_id
  location   = var.region

  labels = {
    project = "uk-flight-punctuality"
  }
}