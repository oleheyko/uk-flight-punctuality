# output "bucket_name" {
#   description = "The name of the Cloud Storage bucket used as the raw data lake."
#   value       = google_storage_bucket.raw_data_lake.name
# }

# output "bigquery_dataset_id" {
#   description = "The BigQuery dataset ID created for flight punctuality data."
#   value       = google_bigquery_dataset.flight_data.dataset_id
# }

# output "cloud_run_url" {
#   description = "The URL of the deployed Cloud Run ingestion service."
#   value       = google_cloud_run_v2_service.cloud_run.uri
# }

# output "scheduler_job_name" {
#   description = "The Cloud Scheduler job name that triggers the Cloud Run service."
#   value       = google_cloud_scheduler_job.trigger_cloud_run.name
# }
