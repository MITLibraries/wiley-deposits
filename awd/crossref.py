import logging
from collections.abc import Iterator

import requests
import smart_open

logger = logging.getLogger(__name__)


def get_dois_from_spreadsheet(file: str) -> Iterator[str]:
    """Retriev DOIs from the Wiley-provided CSV file."""
    with smart_open.open(file, encoding="utf-8-sig") as csvfile:
        yield from csvfile.read().splitlines()


def get_response_from_doi(url: str, doi: str) -> requests.Response:
    """Retrieve Crossref works based on a DOI."""
    logger.debug("Requesting metadata for %s%s", url, doi)
    response = requests.get(
        f"{url}{doi}",
        params={
            "mailto": "dspace-lib@mit.edu",
        },
        timeout=30,
    )
    logger.debug("Response code retrieved from Crossref for %s: %s", doi, response)
    return response
