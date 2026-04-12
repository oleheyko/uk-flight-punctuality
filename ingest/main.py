import logging
from urllib.parse import urljoin
import time

import requests
from google.cloud import bigquery

from config import Config
from scraper import fetch_page, parse_full_analysis_csv_links
from storage import upload_blob_if_missing
from bigquery_utils import ensure_dataset, load_csvs_to_table


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    setup_logging()
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

    # for record in records:
    #     year_dir = record["reporting_period"][:4]
    #     destination_blob = (
    #         f"{config.gcs_prefix.rstrip('/')}/{year_dir}/{record['filename']}"
    #     )
    #     try:
    #         result = upload_blob_if_missing(
    #             bucket_name=config.bucket_name,
    #             blob_name=destination_blob,
    #             source_url=record["url"],
    #             session=session,
    #             overwrite=config.overwrite,
    #             timeout=config.request_timeout,
    #         )
    #         if result == "uploaded":
    #             uploaded += 1
    #         elif result == "skipped":
    #             skipped += 1
    #     except Exception as exc:
    #         failed += 1
    #         logging.exception("Failed to process %s: %s", record["url"], exc)

    #     time.sleep(2)  # Be polite and avoid hammering the server

    # logging.info("Ingest complete: uploaded=%d skipped=%d failed=%d", uploaded, skipped, failed)

    if records:
        bq_client = (
            bigquery.Client(project=config.bigquery_project)
            if config.bigquery_project
            else bigquery.Client()
        )
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
                )
            except Exception as exc:
                logging.exception("Failed to load BigQuery table for year %s: %s", year, exc)
    else:
        logging.info("No records found; skipping BigQuery load.")


if __name__ == "__main__":
    main()
