import logging

import requests

logger = logging.getLogger(__name__)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
}


def get_wiley_response(url, doi):
    """Get response from Wiley server based on a DOI."""
    response = requests.get(f"{url}{doi}", headers=headers)
    logger.info(f"Response retrieved from Wiley server for {doi}")
    return response
