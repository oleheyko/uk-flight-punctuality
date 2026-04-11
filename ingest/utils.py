import re


def extract_reporting_period(text: str) -> str:
    match = re.search(r"(20\d{4})", text)
    if not match:
        return ""

    period = match.group(1)
    year = int(period[:4])
    month = int(period[4:])
    if 2000 <= year <= 2025 and 1 <= month <= 12:
        return period
    return ""


def safe_filename(text: str) -> str:
    normalized = text.strip().lower()
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized)
    normalized = re.sub(r"_{2,}", "_", normalized)
    normalized = normalized.strip("_")
    return normalized


def build_record_filename(link_text: str, reporting_period: str) -> str:
    # Derive a stable CSV filename from the visible link text.
    filename_base = safe_filename(link_text)
    filename_base = filename_base.replace("fullanalysis", "full_analysis")
    if filename_base.endswith("csv_document"):
        filename_base = filename_base[: -len("csv_document")].rstrip("_")
    if not filename_base.endswith("csv"):
        filename_base = f"{filename_base}.csv"

    # Ensure the reporting period is preserved at the beginning.
    if not filename_base.startswith(reporting_period):
        filename_base = f"{reporting_period}_{filename_base}"

    return filename_base
