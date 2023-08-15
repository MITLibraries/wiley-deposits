import json
import logging
from collections.abc import Iterator
from typing import Any

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


def get_metadata_extract_from(work_record: dict[str, Any]) -> dict[str, Any]:
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
    work = work_record["message"]
    value_dict: dict[str, Any] = {}
    for key in [k for k in work if k in keys_for_dspace]:
        if key == "author":
            authors = []
            for author in work["author"]:
                name = f'{author.get("family")}, {author.get("given")}'
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


def create_dspace_metadata_from_dict(
    value_dict: dict[str, Any], metadata_mapping_path: str
) -> dict[str, Any]:
    """Create DSpace JSON metadata from metadata dict and a JSON metadata mapping file."""
    with open(metadata_mapping_path) as metadata_mapping_file:
        metadata_mapping = json.load(metadata_mapping_file)
        metadata = []
        for key in [k for k in metadata_mapping if k in value_dict]:
            if isinstance(value_dict[key], list):
                metadata.extend(
                    [
                        {"key": metadata_mapping[key], "value": list_item}
                        for list_item in value_dict[key]
                    ]
                )
            else:
                metadata.append({"key": metadata_mapping[key], "value": value_dict[key]})
        return {"metadata": metadata}


def is_valid_dspace_metadata(dspace_metadata: dict[str, Any]) -> bool:
    """Validate that the metadata follows the format expected by DSpace."""
    approved_metadata_fields = [
        "dc.contributor.author",
        "dc.relation.journal",
        "dc.identifier.issn",
        "mit.journal.issue",
        "dc.date.issued",
        "dc.language",
        "dc.title.alternative",
        "dc.publisher",
        "dc.title",
        "dc.relation.isversionof",
        "mit.journal.volume",
    ]
    is_valid = False
    if dspace_metadata.get("metadata") is not None:
        for element in dspace_metadata["metadata"]:
            if (
                element.get("key") is not None
                and element.get("value") is not None
                and element.get("key") in approved_metadata_fields
            ):
                is_valid = True
        logger.debug("Valid DSpace metadata generated")
    else:
        logger.exception("Invalid DSpace metadata generated: %s ", dspace_metadata)
    return is_valid


def is_valid_response(doi: str, crossref_response: requests.Response) -> bool:
    """Validate the Crossref work record contains sufficient metadata."""
    validation_status = False
    if work_record := crossref_response.json():
        if (
            work_record.get("message", {}).get("title") is not None
            and work_record.get("message", {}).get("URL") is not None
        ):
            validation_status = True
            logger.debug("Sufficient metadata downloaded for %s", doi)
        else:
            logger.exception("Insufficient metadata for %s, missing title or URL", doi)
    else:
        logger.exception("Unable to parse %s response as JSON", doi)
    return validation_status
