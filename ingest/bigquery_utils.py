import io
import logging
from typing import Optional, Sequence

import pandas as pd
from google.api_core.exceptions import NotFound
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
    except NotFound:
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


def normalize_punctuality_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.drop(columns=["reporting_period", "run_date"], errors="ignore")

    previous_year_columns = [col for col in df.columns if col.startswith("previous_year_")]
    df = df.drop(columns=previous_year_columns, errors="ignore")

    # Rename columns to a standard format
    rename_map = {
        "flights_more_than_15_minutes_early_percent": "more_than_15_mins_early_percent",
        "flights_15_minutes_early_to_1_minute_early_percent": "15_mins_early_to_1_minute_early_percent",
        "flights_0_to_15_minutes_late_percent": "0_to_15_mins_late_percent",
        "flts_16_to_30_mins_late_percent": "16_to_30_mins_late_percent",
        "flts_31_to_60_mins_late_percent": "31_to_60_mins_late_percent",
        "flts_61_to_180_mins_late_percent": "61_to_180_mins_late_percent",
        "flts_181_to_360_mins_late_percent": "181_to_360_mins_late_percent",
        "flights_between_16_and_30_minutes_late_percent": "16_to_30_mins_late_percent",
        "flights_between_31_and_60_minutes_late_percent": "31_to_60_mins_late_percent",
        "flights_between_61_and_120_minutes_late_percent": "61_to_120_mins_late_percent",
        "flights_between_121_and_180_minutes_late_percent": "121_to_180_mins_late_percent",
        "flights_between_181_and_360_minutes_late_percent": "181_to_360_mins_late_percent",
        "flights_more_than_360_minutes_late_percent": "more_than_360_mins_late_percent"
    }

    for source, target in rename_map.items():
        if source in df.columns:
            if target in df.columns:
                df[target] = df[target].fillna(df[source])
                df = df.drop(columns=[source])
            else:
                df = df.rename(columns={source: target})

    # Handle cases where 61_to_180_mins_late_percent is missing
    mask = df["61_to_180_mins_late_percent"].isna()

    # then do the operation only for those rows
    df.loc[mask, "61_to_180_mins_late_percent"] = (
        df.loc[mask, "61_to_120_mins_late_percent"]
        + df.loc[mask, "121_to_180_mins_late_percent"]
    )

    # Handle cases where early_to_15_mins_late_percent is missing
    mask = df["early_to_15_mins_late_percent"].isna()
    df.loc[mask, "early_to_15_mins_late_percent"] = (
        df.loc[mask, "15_mins_early_to_1_minute_early_percent"]
        + df.loc[mask, "0_to_15_mins_late_percent"]
        + df.loc[mask, "more_than_15_mins_early_percent"]  # or whatever source expression you need
    )

    final_columns = [
        "reporting_airport",
        "origin_destination_country",
        "origin_destination",
        "airline_name",
        "scheduled_charter",
        "year",
        "month",
        "number_flights_matched",
        "actual_flights_unmatched",
        "more_than_15_mins_early_percent",
        "15_mins_early_to_1_minute_early_percent",
        "0_to_15_mins_late_percent",
        "early_to_15_mins_late_percent",
        "16_to_30_mins_late_percent",
        "31_to_60_mins_late_percent",
        "61_and_120_mins_late_percent",
        "121_and_180_mins_late_percent",
        "61_to_180_mins_late_percent",
        "181_to_360_mins_late_percent",
        "more_than_360_mins_late_percent",
        "flights_unmatched_percent",
        "flights_cancelled_percent",
        "number_flights_cancelled",
        "average_delay_mins",
        "planned_flights_unmatched",
    ]

    ordered_columns = [col for col in final_columns if col in df.columns]
    # extra_columns = [col for col in df.columns if col not in ordered_columns]
    return df[ordered_columns]


def get_yearly_punctuality_table_names(
    client: bigquery.Client,
    dataset_id: str,
    table_prefix: str,
) -> list[str]:
    dataset_ref = client.dataset(dataset_id)
    table_names = [
        table.table_id
        for table in client.list_tables(dataset_ref)
        if table.table_id.startswith(table_prefix)
    ]
    if not table_names:
        raise ValueError(
            f"No BigQuery tables found in dataset {dataset_id} with prefix {table_prefix}"
        )
    return sorted(table_names)


def load_normalized_union_table(
    client: bigquery.Client,
    dataset_id: str,
    table_prefix: str,
    destination_table_name: str = "punctuality_data_all_years",
    source_table_names: Optional[Sequence[str]] = None,
) -> bigquery.LoadJob:
    if source_table_names is None:
        source_table_names = get_yearly_punctuality_table_names(
            client=client,
            dataset_id=dataset_id,
            table_prefix=table_prefix,
        )

    # Always exclude the destination table from the source list to avoid
    # reading the already-normalized union back into the union operation.
    if destination_table_name in source_table_names:
        logging.info(
            "Excluding destination table %s from source tables list",
            destination_table_name,
        )
        source_table_names = [t for t in source_table_names if t != destination_table_name]

    raw_frames = []
    for table_name in source_table_names:
        logging.info("Reading BigQuery table %s.%s", dataset_id, table_name)
        table_ref = client.dataset(dataset_id).table(table_name)
        df = client.list_rows(table_ref).to_dataframe(create_bqstorage_client=False)
        raw_frames.append(df)

    if not raw_frames:
        raise ValueError("No yearly tables were loaded for normalization")

    unioned_raw = pd.concat(raw_frames, ignore_index=True, sort=False)
    normalized = normalize_punctuality_dataframe(unioned_raw)

    destination_ref = client.dataset(dataset_id).table(destination_table_name)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
    )

    logging.info(
        "Loading normalized unioned BigQuery table %s.%s (%s rows)",
        dataset_id,
        destination_table_name,
        len(normalized),
    )
    job = client.load_table_from_dataframe(normalized, destination_ref, job_config=job_config)
    job.result()
    logging.info(
        "Normalized BigQuery table loaded: %s.%s (%s rows)",
        dataset_id,
        destination_table_name,
        job.output_rows,
    )
    return job


def load_csvs_to_table(
    client: bigquery.Client,
    bucket_name: str,
    gcs_prefix: str,
    year: str,
    dataset_id: str,
    table_name: str,
    schema_fields: Optional[Sequence[bigquery.SchemaField]] = None,
    storage_client: Optional[storage.Client] = None,
    skip_if_table_exists: bool = False,
) -> Optional[bigquery.LoadJob]:
    storage_client = storage_client or storage.Client()

    table_ref = client.dataset(dataset_id).table(table_name)
    if skip_if_table_exists:
        try:
            client.get_table(table_ref)
            logging.info(
                "Skipping BigQuery load because table already exists: %s.%s",
                dataset_id,
                table_name,
            )
            return None
        except NotFound:
            pass

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
