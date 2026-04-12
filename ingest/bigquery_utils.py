import logging
from typing import Optional, Sequence

from google.cloud import bigquery


def ensure_dataset(
    client: bigquery.Client,
    dataset_id: str,
    location: Optional[str] = None,
) -> None:
    dataset_ref = bigquery.Dataset(client.dataset(dataset_id))

    try:
        client.get_dataset(dataset_ref.reference)
        logging.info("BigQuery dataset already exists: %s", dataset_id)
        return
    except Exception:
        pass

    if not location:
        raise ValueError("BIGQUERY_LOCATION is required when creating a new dataset")

    dataset_ref.location = location
    dataset_ref = client.create_dataset(dataset_ref, exists_ok=True)
    logging.info("Created BigQuery dataset: %s", dataset_ref.dataset_id)


def load_csvs_to_table(
    client: bigquery.Client,
    bucket_name: str,
    gcs_prefix: str,
    year: str,
    dataset_id: str,
    table_name: str,
    schema_fields: Optional[Sequence[bigquery.SchemaField]] = None,
) -> bigquery.LoadJob:
    # Clean the GCS prefix string for path construction only.
    prefix = gcs_prefix.strip("/")
    if prefix:
        source_uri = f"gs://{bucket_name}/{prefix}/{year}/*.csv"
    else:
        source_uri = f"gs://{bucket_name}/{year}/*.csv"

    table_ref = client.dataset(dataset_id).table(table_name)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
    )

    logging.info(
        "Starting BigQuery load for year %s into %s.%s from %s",
        year,
        dataset_id,
        table_name,
        source_uri,
    )
    job = client.load_table_from_uri(source_uri, table_ref, job_config=job_config)
    job.result()
    logging.info(
        "BigQuery load complete for year %s into %s.%s (%s rows)",
        year,
        dataset_id,
        table_name,
        job.output_rows,
    )
    return job
