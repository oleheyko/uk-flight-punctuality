import os
from datetime import datetime


def parse_bool(value: str, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


class Config:
    def __init__(
        self,
        bucket_name: str,
        caa_base_url: str,
        start_year: int,
        end_year: int,
        gcs_prefix: str,
        overwrite: bool,
        request_timeout: int,
        bigquery_project: str,
        bigquery_dataset: str,
        bigquery_table_prefix: str,
        bigquery_location: str,
    ):
        self.bucket_name = bucket_name
        self.caa_base_url = caa_base_url
        self.start_year = start_year
        self.end_year = end_year
        self.gcs_prefix = gcs_prefix
        self.overwrite = overwrite
        self.request_timeout = request_timeout
        self.bigquery_project = bigquery_project
        self.bigquery_dataset = bigquery_dataset
        self.bigquery_table_prefix = bigquery_table_prefix
        self.bigquery_location = bigquery_location

    @classmethod
    def from_env(cls) -> "Config":
        bucket_name = os.getenv("BUCKET_NAME", "").strip()
        if not bucket_name:
            raise ValueError("BUCKET_NAME is required")

        caa_base_url = os.getenv("CAA_BASE_URL", "https://www.caa.co.uk/data-and-analysis/uk-aviation-market/flight-punctuality/uk-flight-punctuality-statistics/")

        start_year = int(os.getenv("CAA_YEAR_START", "2000"))
        end_year = int(os.getenv("CAA_YEAR_END", datetime.now().year))
        if start_year < 2000 or end_year < start_year:
            raise ValueError("CAA_YEAR_START must be >= 2000 and CAA_YEAR_END must be >= CAA_YEAR_START")

        gcs_prefix = os.getenv("GCS_PREFIX", "raw/").strip()
        overwrite = parse_bool(os.getenv("OVERWRITE", "false"), default=False)
        request_timeout = int(os.getenv("REQUEST_TIMEOUT", "60"))

        bigquery_project = os.getenv("BIGQUERY_PROJECT", "").strip()
        bigquery_dataset = os.getenv("BIGQUERY_DATASET", "flight_data").strip()
        bigquery_table_prefix = os.getenv("BIGQUERY_TABLE_PREFIX", "punctuality_data_").strip()
        bigquery_location = os.getenv("BIGQUERY_LOCATION", "EU").strip()

        return cls(
            bucket_name=bucket_name,
            caa_base_url=caa_base_url,
            start_year=start_year,
            end_year=end_year,
            gcs_prefix=gcs_prefix,
            overwrite=overwrite,
            request_timeout=request_timeout,
            bigquery_project=bigquery_project,
            bigquery_dataset=bigquery_dataset,
            bigquery_table_prefix=bigquery_table_prefix,
            bigquery_location=bigquery_location,
        )
