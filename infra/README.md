# GCP Terraform Baseline

This directory provisions a simple GCP baseline for a data engineering portfolio project.

It creates:
- one Cloud Storage bucket for raw UK flight punctuality data
- one BigQuery dataset for analytics
- one Cloud Run job for the ingestion app
- one service account to run the ingestion job
- one one-time invocation of the Cloud Run job on apply

## Usage

1. Copy `terraform.tfvars.example` to `terraform.tfvars` and update values.
2. Run:

```bash
cd infra
terraform init
terraform plan
terraform apply
```

## Notes

- Replace `container_image` with your own Artifact Registry image once the ingestion app is ready.
- This setup now uses a Cloud Run job instead of a Cloud Run service.
- GitHub Actions executes the existing Cloud Run job on a schedule using `.github/workflows/scheduled-gcp-run.yml`.
- Terraform also triggers the Cloud Run job once during apply if the `gcloud` CLI is available.
