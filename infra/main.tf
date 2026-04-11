

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
  location   = upper(var.region)

  labels = {
    project = "uk-flight-punctuality"
  }
}


# resource "google_project_service" "service_usage" {
#   service = "serviceusage.googleapis.com"
# }

# resource "google_project_service" "cloud_run" {
#   service = "run.googleapis.com"
# }

# resource "google_project_service" "cloud_build" {
#   service = "cloudbuild.googleapis.com"
# }

# resource "google_project_service" "cloud_scheduler" {
#   service = "cloudscheduler.googleapis.com"
# }

# resource "google_project_service" "bigquery" {
#   service = "bigquery.googleapis.com"
# }

# resource "google_project_service" "storage" {
#   service = "storage.googleapis.com"
# }

# resource "google_project_service" "iam" {
#   service = "iam.googleapis.com"
# }





# resource "google_service_account" "cloud_run" {
#   account_id   = "cloud-run-ingest"
#   display_name = "Cloud Run service account for ingestion"
# }

# resource "google_service_account" "scheduler" {
#   account_id   = "cloud-scheduler-invoker"
#   display_name = "Scheduler identity to invoke Cloud Run"
# }

# resource "google_cloud_run_v2_service" "cloud_run" {
#   name     = var.service_name
#   location = var.region

#   template {
#     service_account = google_service_account.cloud_run.email

#     containers {
#       image = var.container_image
#     }
#   }

#   labels = {
#     project = "caa-flight-punctuality"
#   }
# }

# resource "google_cloud_run_v2_service_iam_member" "scheduler_invoker" {
#   service  = google_cloud_run_v2_service.cloud_run.name
#   location = google_cloud_run_v2_service.cloud_run.location
#   project  = var.project_id
#   role     = "roles/run.invoker"
#   member   = "serviceAccount:${google_service_account.scheduler.email}"
# }

# resource "google_cloud_scheduler_job" "trigger_cloud_run" {
#   name     = var.scheduler_job_name
#   project  = var.project_id
#   location = var.region
#   description = "Trigger Cloud Run ingestion service on a schedule."
#   schedule    = var.scheduler_cron
#   time_zone   = var.scheduler_time_zone

#   labels = {
#     project = "caa-flight-punctuality"
#   }

#   http_target {
#     http_method = "POST"
#     uri         = google_cloud_run_v2_service.cloud_run.uri
#     body        = jsonencode({ trigger = "scheduler" })

#     oidc_token {
#       service_account_email = google_service_account.scheduler.email
#       audience              = google_cloud_run_v2_service.cloud_run.uri
#     }
#   }

#   depends_on = [google_cloud_run_v2_service_iam_member.scheduler_invoker]
# }
