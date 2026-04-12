import io
import logging
from typing import Optional, Sequence

import pandas as pd
from google.cloud import bigquery, storage


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


def parse_reporting_period(values: pd.Series) -> tuple[pd.Series, pd.Series]:
    period_str = values.astype(str).str.strip()
    valid = period_str.str.match(r"^(\d{4})-?(\d{2})$")
    if not valid.all():
        bad_values = values[~valid].unique().tolist()
        raise ValueError(
            f"Unable to parse reporting_period values. Expected YYYYMM or YYYY-MM format: {bad_values}"
        )

    year = period_str.str.slice(0, 4).astype(int)
    month = period_str.str.replace("-", "", regex=False).str.slice(4, 6).astype(int)
    invalid_months = ~month.between(1, 12)
    if invalid_months.any():
        bad_values = values[invalid_months].unique().tolist()
        raise ValueError(f"reporting_period month must be 01-12: {bad_values}")

    return year, month


def parse_punctuality_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    df = df.copy()
    if "reporting_period" not in df.columns:
        raise ValueError("CSV must include a reporting_period column to derive year and month")

    df["year"], df["month"] = parse_reporting_period(df["reporting_period"])
    df = df.drop(columns=["run_date"], errors="ignore")

    return df


def read_csv_with_fallback(content: bytes) -> pd.DataFrame:
    for encoding in ("utf-8", "latin-1", "cp1252"):
        try:
            return pd.read_csv(io.BytesIO(content), encoding=encoding)
        except UnicodeDecodeError:
            continue

    logging.warning(
        "CSV bytes could not be decoded with utf-8; falling back to latin-1 with replacement"
    )
    return pd.read_csv(io.BytesIO(content), encoding="latin-1", errors="replace")


def load_csvs_to_table(
    client: bigquery.Client,
    bucket_name: str,
    gcs_prefix: str,
    year: str,
    dataset_id: str,
    table_name: str,
    schema_fields: Optional[Sequence[bigquery.SchemaField]] = None,
    storage_client: Optional[storage.Client] = None,
) -> bigquery.LoadJob:
    storage_client = storage_client or storage.Client()

    prefix = gcs_prefix.strip("/")
    if prefix:
        prefix = f"{prefix}/{year}/"
    else:
        prefix = f"{year}/"

    blobs = [
        blob
        for blob in storage_client.list_blobs(bucket_name, prefix=prefix)
        if blob.name.lower().endswith(".csv")
    ]  # Only consider CSV files to avoid processing unrelated files in the same directory
    if not blobs:
        raise ValueError(f"No CSV files found in gs://{bucket_name}/{prefix}")

    frames = []
    for blob in blobs:
        logging.info("Downloading CSV from gs://%s/%s", bucket_name, blob.name)
        content = blob.download_as_bytes()
        df = read_csv_with_fallback(content)
        frames.append(parse_punctuality_dataframe(df))  # Parse and normalize each CSV into a DataFrame

    data_frame = pd.concat(frames, ignore_index=True)
    table_ref = client.dataset(dataset_id).table(table_name)
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
    )

    logging.info(
        "Starting BigQuery load for year %s into %s.%s (%s rows)",
        year,
        dataset_id,
        table_name,
        len(data_frame),
    )
    job = client.load_table_from_dataframe(data_frame, table_ref, job_config=job_config)
    job.result()
    logging.info(
        "BigQuery load complete for year %s into %s.%s (%s rows)",
        year,
        dataset_id,
        table_name,
        job.output_rows,
    )
    return job
