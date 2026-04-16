# uk-flight-punctionality

A UK flight punctuality ingestion project that downloads CAA monthly CSV files, uploads them to Google Cloud Storage, and loads them into BigQuery.

## Root setup helper

Use `set_up.py` to create a local root `.env` file, build the ingestion and dbt Docker images from `ingest/Dockerfile` and `dbt/Dockerfile`, and push them to Google Artifact Registry.

```bash
python set_up.py --project YOUR_PROJECT_ID --repo YOUR_ARTIFACT_REGISTRY_REPO
```

Or use an explicit image URI:

```bash
python set_up.py --image europe-west2-docker.pkg.dev/YOUR_PROJECT_ID/YOUR_REPO/uk-flight-ingest:latest
```

After the image is pushed:

1. Update `infra/terraform.tfvars` with the pushed image URI under `container_image`.
2. Run Terraform from `infra/`:
   ```bash
   cd infra
   terraform init
   terraform apply
   ```

## Recommended workflow

1. Build and push the container image with `set_up.py`.
2. Provision GCP resources with Terraform in `infra/`.
3. Verify the Cloud Run job and then let GitHub Actions invoke it on schedule.

## GitHub scheduled runs

A scheduled workflow has been added at `.github/workflows/scheduled-gcp-run.yml`.
It runs on a cron schedule and can also be triggered manually from the GitHub UI.

Configure the following repository secrets:
- `GCP_PROJECT`
- `GCP_REGION`
- `GCP_JOB_NAME`
- `GCP_SA_KEY`

`GCP_SA_KEY` should contain the JSON key for a service account with permissions to execute Cloud Run jobs.

## Notes

- `set_up.py` only handles local image build and push.
- Resource provisioning is handled separately by Terraform in `infra/`.
