import requests

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
}


def get_wiley_pdf(url, doi):
    """Get PDF from Wiley server based on a DOI."""
    return requests.get(f"{url}{doi}", headers=headers)
