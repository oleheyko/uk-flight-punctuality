variable "credentials" {
  description = "GCP Credentials"
  default     = "./keys/my-creds.json"
}

variable "project_id" {
  description = "GCP project ID where the infrastructure will be created."
  default     = "uk-flight-punctuality-492918"
}

variable "region" {
  description = "GCP region for Cloud Run, Cloud Scheduler, and storage location."
  default     = "europe-west2"
}

variable "bucket_name" {
  description = "Name of the Cloud Storage bucket used as the raw data lake."
  default     = "uk-flight-punctuality-raw-data-lake"
}

variable "dataset_id" {
  description = "BigQuery dataset ID used for storing flight punctuality data."
  default     = "flight_data"
}

# variable "service_name" {
#   description = "Cloud Run service name for the ingestion application."
#   type        = string
# }

# variable "scheduler_job_name" {
#   description = "Cloud Scheduler job name that triggers the Cloud Run service."
#   type        = string
# }

# variable "scheduler_cron" {
#   description = "Cron schedule expression for the scheduler job."
#   type        = string
#   default     = "0 6 * * *"
# }

# variable "scheduler_time_zone" {
#   description = "Time zone used by Cloud Scheduler for the cron schedule."
#   type        = string
#   default     = "Europe/London"
# }

# variable "container_image" {
#   description = "Placeholder container image for the Cloud Run ingestion service. Replace with your own image later."
#   type        = string
# }
