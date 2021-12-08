import json
import logging

import requests
import smart_open

logger = logging.getLogger(__name__)


def get_dois_from_spreadsheet(file):
    """Retriev DOIs from the Wiley-provided CSV file."""
    with smart_open.open(file, encoding="utf-8-sig") as csvfile:
        for doi in csvfile.read().splitlines():
            yield doi


def get_work_record_from_doi(api_url, doi):
    """Retrieve Crossref works based on a DOI"""
    crossref_work_record = requests.get(
        f"{api_url}{doi}", params={"mailto": "dspace-lib@mit.edu"}
    ).json()
    return crossref_work_record


def get_metadata_extract_from(work):
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


def create_dspace_metadata_from_dict(value_dict, metadata_mapping_path):
    """Create DSpace JSON metadata from metadata dict and a JSON metadata mapping file."""
    with open(metadata_mapping_path, "r") as metadata_mapping:
        metadata_mapping = json.load(metadata_mapping)
        metadata = []
        for key in [k for k in metadata_mapping if k in value_dict.keys()]:
            if isinstance(value_dict[key], list):
                for list_item in value_dict[key]:
                    metadata.append({"key": metadata_mapping[key], "value": list_item})
            else:
                metadata.append(
                    {"key": metadata_mapping[key], "value": value_dict[key]}
                )
        return {"metadata": metadata}


def is_valid_dspace_metadata(dspace_metadata):
    """Validate that the metadata follows the format expected by DSpace."""
    validation_status = False
    if dspace_metadata.get("metadata") is not None:
        for element in dspace_metadata["metadata"]:
            if element.get("key") is not None and element.get("value") is not None:
                validation_status = True
        logger.debug("Valid DSpace metadata generated")
    else:
        logger.error(f"Invalid DSpace metadata generated: {dspace_metadata}")
    return validation_status


def is_valid_response(doi, crossref_work_record):
    """Validate the Crossref work record contains sufficient metadata."""
    validation_status = False
    if (
        crossref_work_record.get("message", {}).get("title") is not None
        and crossref_work_record.get("message", {}).get("URL") is not None
    ):
        validation_status = True
        logger.debug(f"Sufficient metadata downloaded for {doi}")
    else:
        logger.error(f"Insufficient metadata for {doi}, missing title or URL")
    return validation_status
