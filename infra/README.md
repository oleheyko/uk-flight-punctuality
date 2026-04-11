# GCP Terraform Baseline

This directory provisions a simple GCP baseline for a data engineering portfolio project.

It creates:
- one Cloud Storage bucket for raw UK flight punctuality data
- one BigQuery dataset for analytics
- one Cloud Run service for the ingestion app
- one Cloud Scheduler job to invoke Cloud Run on a schedule
- one service account to authenticate Scheduler to Cloud Run

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

- Replace `container_image` with your own Cloud Run image once the ingestion app is ready.
- The scheduler job sends a POST request with a small JSON body to Cloud Run using OIDC authentication.
