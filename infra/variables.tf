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
