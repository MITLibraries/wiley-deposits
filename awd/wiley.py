import logging

import requests

logger = logging.getLogger(__name__)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
}


def get_wiley_response(url: str, doi: str) -> requests.Response:
    """Get response from Wiley server based on a DOI."""
    logger.debug("Requesting PDF for %s%s", url, doi)
    response = requests.get(f"{url}{doi}", headers=headers, timeout=30)
    logger.debug("Response code retrieved from Wiley server for %s: %s", doi, response)
    return response
