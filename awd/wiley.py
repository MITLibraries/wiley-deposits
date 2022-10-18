import logging

import requests

logger = logging.getLogger(__name__)

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
}


def get_wiley_response(url, doi):
    """Get response from Wiley server based on a DOI."""
    logger.debug("Requesting PDF for %s%s", url, doi)
    response = requests.get(f"{url}{doi}", headers=headers)
    logger.debug("Response code retrieved from Wiley server for %s: %s", doi, response)
    return response


def is_valid_response(doi, wiley_response):
    """Validate the Wiley response contained a PDF."""
    validation_status = False
    if wiley_response.headers["content-type"] == "application/pdf; charset=UTF-8":
        validation_status = True
        logger.debug(f"PDF downloaded for {doi}")
    else:
        logger.error(f"A PDF could not be retrieved for DOI: {doi}")
        logger.debug(
            "Response contents retrieved from Wiley server for %s: %s",
            doi,
            wiley_response.content,
        )
    return validation_status
