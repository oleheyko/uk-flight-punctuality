import logging
import re
from typing import Dict, List
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from utils import build_record_filename, extract_reporting_period

pattern = re.compile(
    r'^(?!.*(?:Arrival\s*Departure|Arr\s*Dep|ArrDep))(?=.*Full\s*Analysis).*$',
    re.IGNORECASE
)


def fetch_page(session, url: str, timeout: int) -> str:
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text


def parse_full_analysis_csv_links(html: str, base_url: str) -> List[Dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    anchors = soup.find_all("a")

    logging.info("Total anchor tags found: %d", len(anchors))

    records = []
    for anchor in anchors:
        link_text = anchor.get_text(" ", strip=True)
        if not link_text:
            continue

        href = anchor.get("href")
        if href is None:
            continue
        href = str(href)

        # Match only monthly full analysis CSV documents from the year range.
        if not pattern.search(link_text):
            continue

        reporting_period = extract_reporting_period(link_text)
        if not reporting_period:
            logging.debug("Skipping link because reporting period could not be extracted: %s", link_text)
            continue

        url = urljoin(base_url, href)
        filename = build_record_filename(link_text, reporting_period)

        records.append(
            {
                "title": link_text,
                "url": url,
                "reporting_period": reporting_period,
                "filename": filename,
            }
        )

    return records
