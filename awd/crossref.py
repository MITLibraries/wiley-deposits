import requests
import smart_open


def get_dois_from_spreadsheet(file):
    """Retriev DOIs from the Wiley-provided CSV file."""
    with smart_open.open(file, encoding="utf-8-sig") as csvfile:
        for doi in csvfile.read().splitlines():
            yield doi


def get_crossref_work_from_doi(api_url, doi):
    """Retrieve Crossref works based on a DOI"""
    work = requests.get(
        f"{api_url}{doi}", params={"mailto": "dspace-lib@mit.edu"}
    ).json()
    return work


def get_metadata_dict_from_crossref_work(work):
    """Create metadata dict from a Crossref work JSON record."""
    keys_for_dspace = [
        "author",
        "container-title",
        "ISSN",
        "issue",
        "issued",
        "language",
        "original-title",
        "publisher",
        "short-title",
        "subtitle",
        "title",
        "URL",
        "volume",
    ]
    work = work["message"]
    value_dict = {}
    for key in [k for k in work.keys() if k in keys_for_dspace]:
        if key == "author":
            authors = []
            for author in work["author"]:
                name = f'{author["family"]}, {author["given"]}'
                authors.append(name)
            value_dict[key] = authors
        elif key == "title":
            value_dict[key] = ". ".join(t for t in work[key])
        elif key == "issued":
            issued = "-".join(str(d).zfill(2) for d in work["issued"]["date-parts"][0])
            value_dict[key] = issued
        else:
            value_dict[key] = work[key]
    return value_dict
