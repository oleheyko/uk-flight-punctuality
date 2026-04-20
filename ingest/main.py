import argparse
import logging
import os
from pathlib import Path
from urllib.parse import urljoin
import time

import requests
from google.cloud import bigquery, storage

from config import Config
from scraper import fetch_page, parse_full_analysis_csv_links
from storage import list_blob_names, upload_blob_if_missing
from bigquery_utils import (
    ensure_dataset,
    load_csvs_to_table,
    load_normalized_union_table,
)


def load_dotenv_file(env_path: Path) -> None:
    if not env_path.exists():
        return

    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="UK flight punctuality ingest pipeline"
    )
    parser.add_argument(
        "--normalize-all-years",
        action="store_true",
        help=(
            "Build the normalized unioned BigQuery table from already-loaded yearly tables "
            "instead of downloading/uploading CSV files."
        ),
    )
    return parser.parse_args()


def main() -> None:
    setup_logging()
    # Run normalization by default when executing the script directly
    # Try loading .env from root first, then fall back to local folder (for container mounting)
    root_env = Path(__file__).resolve().parent.parent / ".env"
    local_env = Path(__file__).resolve().parent / ".env"
    load_dotenv_file(root_env)
    load_dotenv_file(local_env)
    config = Config.from_env()

    logging.info(
        "Starting ingest for monthly punctuality full analysis CSV files from %s to %s",
        config.start_year,
        config.end_year,
    )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Safari/605.1.15"
            )
        }
    )

    records = []
    seen_filenames = set()
    pages_scanned = 0

    for year in range(config.start_year, config.end_year + 1):
        page_url = urljoin(config.caa_base_url.rstrip("/") + "/", f"{year}/")
        try:
            html = fetch_page(session, page_url, timeout=config.request_timeout)
        except Exception as exc:
            logging.warning("Failed to fetch source page %s: %s", page_url, exc)
            continue

        pages_scanned += 1
        year_records = parse_full_analysis_csv_links(html, page_url)
        for record in year_records:
            if record["filename"] in seen_filenames:
                logging.debug("Skipping duplicate filename %s from %s", record["filename"], page_url)
                continue
            seen_filenames.add(record["filename"])
            records.append(record)

    logging.info("Pages scanned: %d", pages_scanned)
    logging.info("Total links matched: %d", len(records))

    uploaded = 0
    skipped = 0
    failed = 0

    storage_client = storage.Client()
    gcs_prefix = config.gcs_prefix.rstrip("/")
    if gcs_prefix:
        gcs_prefix = f"{gcs_prefix}/"

    existing_blob_names = list_blob_names(
        bucket_name=config.bucket_name,
        prefix=gcs_prefix,
        client=storage_client,
    )

    for record in records:
        year_dir = record["reporting_period"][:4]
        destination_blob = f"{gcs_prefix}{year_dir}/{record['filename']}"

        if not config.overwrite and destination_blob in existing_blob_names:
            logging.info("Skipping existing blob: %s", destination_blob)
            skipped += 1
            continue

        try:
            result = upload_blob_if_missing(
                bucket_name=config.bucket_name,
                blob_name=destination_blob,
                source_url=record["url"],
                session=session,
                overwrite=config.overwrite,
                timeout=config.request_timeout,
                client=storage_client,
                skip_exists_check=not config.overwrite,
            )
            if result == "uploaded":
                uploaded += 1
                existing_blob_names.add(destination_blob)
            elif result == "skipped":
                skipped += 1
        except Exception as exc:
            failed += 1
            logging.exception("Failed to process %s: %s", record["url"], exc)

        time.sleep(2)  # Be polite and avoid hammering the server

    logging.info("Ingest complete: uploaded=%d skipped=%d failed=%d", uploaded, skipped, failed)

    if records:
        bq_client = (
            bigquery.Client(project=config.bigquery_project)
            if config.bigquery_project
            else bigquery.Client()
        )
        # Ensure the BigQuery dataset exists before loading tables
        ensure_dataset(
            client=bq_client,
            dataset_id=config.bigquery_dataset,
            location=config.bigquery_location,
        )

        years = sorted({record["reporting_period"][:4] for record in records})
        for year in years:
            table_name = f"{config.bigquery_table_prefix}{year}"
            try:
                load_csvs_to_table(
                    client=bq_client,
                    bucket_name=config.bucket_name,
                    gcs_prefix=config.gcs_prefix,
                    year=year,
                    dataset_id=config.bigquery_dataset,
                    table_name=table_name,
                    skip_if_table_exists=not config.overwrite,
                )
            except Exception as exc:
                logging.exception("Failed to load BigQuery table for year %s: %s", year, exc)

        logging.info(
            "Building normalized unioned BigQuery table from existing yearly tables in %s",
            config.bigquery_dataset,
        )
        try:
            load_normalized_union_table(
                client=bq_client,
                dataset_id=config.bigquery_dataset,
                table_prefix=config.bigquery_table_prefix,
            )
        except Exception as exc:
            logging.exception(
                "Failed to build normalized unioned BigQuery table: %s",
                exc,
            )
    else:
        logging.info("No records found; skipping BigQuery load.")


if __name__ == "__main__":
    main()
