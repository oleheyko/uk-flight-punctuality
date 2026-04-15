from datetime import datetime

from ingest.utils import extract_reporting_period


def test_extract_reporting_period_includes_current_year():
    current_year = datetime.now().year
    period = f"{current_year}05"

    assert extract_reporting_period(f"Punctuality Full Analysis {period}") == period


def test_extract_reporting_period_allows_dynamic_future_years():
    current_year = datetime.now().year
    next_year = current_year + 1
    period = f"{next_year}05"

    assert extract_reporting_period(f"Punctuality Full Analysis {period}") == ""
