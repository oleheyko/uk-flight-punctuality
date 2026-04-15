# UK Flight Punctuality Ingest

This pipeline downloads monthly **Punctuality Statistics Full Analysis (CSV document)** files from CAA year pages between 2000 and 2025 and uploads them to Google Cloud Storage.

## What it ingests

- Only monthly files with titles like `201104 FullAnalysis (CSV document)` or `202502 Punctuality Statistics Full Analysis (CSV document)`
- Only files from CAA year directories between 2000 and 2025
- Only CSV full analysis documents
- Excludes annual reports, summary analysis files, XLSX, PDF, and non-full-analysis files

## Required environment variables

- `BUCKET_NAME` - GCS bucket name
- `CAA_BASE_URL` - base page URL prefix, for example `https://www.caa.co.uk/data-and-analysis/uk-aviation-market/flight-punctuality/uk-flight-punctuality-statistics/`
- `CAA_2025_URL` - optional fallback source URL for 2025; used only if `CAA_BASE_URL` is not set
- `CAA_YEAR_START` - first year to scan, default `2000`
- `CAA_YEAR_END` - last year to scan, default `2025`
- `GCS_PREFIX` - destination prefix inside the bucket, e.g. `raw/`
  Files are uploaded to year folders under this prefix, for example `raw/2025/`.
- `OVERWRITE` - `true` or `false` to control re-upload behavior
- `REQUEST_TIMEOUT` - request timeout in seconds for HTTP calls
- `BIGQUERY_PROJECT` - optional BigQuery project ID; if omitted, client defaults are used
- `BIGQUERY_DATASET` - BigQuery dataset name, default `flight_data`
- `BIGQUERY_TABLE_PREFIX` - BigQuery table prefix for yearly tables, default `punctuality_data_`
- `BIGQUERY_LOCATION` - BigQuery dataset location for creation, default `EU`

## BigQuery behavior

After raw files are uploaded to GCS, the pipeline loads yearly CSV data into BigQuery tables inside the configured dataset. During ingest, the pipeline drops the original `run_date` column and adds `year` and `month` derived from the CSV `reporting_period` field. For example, it creates or replaces tables named `punctuality_data_2000`, `punctuality_data_2001`, etc.

## How to run locally

1. Create a local root `.env` file using `.env.example` or export the variables in your shell.
2. From the `ingest/` directory run:

```sh
python main.py
```

Or use the helper script:

```sh
./run_local.sh
```

### Running in Docker

Build the container from the `ingest/` directory:

```sh
docker build -t uk-flight-ingest .
```

Run the container with environment variables and a mounted Google credentials file:

```sh
docker run --rm \
  -e BUCKET_NAME=your-bucket-name \
  -e CAA_BASE_URL=https://www.caa.co.uk/... \
  -e GOOGLE_APPLICATION_CREDENTIALS=/creds/gcloud-key.json \
  -v /local/path/to/gcloud-key.json:/creds/gcloud-key.json:ro \
  uk-flight-ingest
```

If you already have a local `.env` file at the project root, you can pass it with `--env-file ../.env` from the `ingest/` directory, but the Google service account key must still be mounted into the container and referenced by `GOOGLE_APPLICATION_CREDENTIALS`.

### Running with Docker Compose

Create or place your service account key at `ingest/gcloud-key.json`, then run from the `ingest/` directory:

```sh
docker compose up --build
```

The compose service uses `env_file: ../.env` and sets `GOOGLE_APPLICATION_CREDENTIALS=/creds/gcloud-key.json`.

If you prefer to use a different path for the key file, update `docker-compose.yml` or pass a custom bind mount instead.

To build the normalized unioned BigQuery table from the already-loaded yearly tables, run:

```sh
python main.py --normalize-all-years
```

## Destination GCS path

Files are uploaded to:

```text
gs://[BUCKET_NAME]/raw/2025/
```

Example destination blob:

```text
raw/2025/202501_punctuality_statistics_summary_analysis.csv
```
