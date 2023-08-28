import json
import logging
from typing import Any

from requests import Response

from awd.crossref import get_response_from_doi
from awd.database import DoiProcessAttempt
from awd.wiley import get_wiley_response

logger = logging.getLogger(__name__)


class Article:
    """Article class.

    Class represents articles published by Wiley for which we will attempt to get
    metadata and binary content for uploading to our DSpace repository.
    """

    def __init__(
        self,
        doi: str,
        metadata_url: str,
        content_url: str,
        doi_table: DoiProcessAttempt,
    ) -> None:
        """Initialize article instance.

        Args:
            doi: A digital object identifer (doi) for an article.
            metadata_url: The URL for retrieving metadata records.
            content_url: The URL for retrieving article content.
            doi_table: The DOI table as a PynamoDB object.
        """
        self.doi: str = doi
        self.metadata_url: str = metadata_url
        self.content_url: str = content_url
        self.doi_table: DoiProcessAttempt = doi_table
        self.crossref_metadata: dict[str, Any]
        self.dspace_metadata: dict[str, Any]
        self.article_content: bytes

    def process(self) -> None:
        """Run the complete article processing workflow."""
        self.doi_table.check_doi_and_add_to_table(self.doi)
        self.get_and_validate_crossref_metadata()
        self.create_and_validate_dspace_metadata()
        self.get_and_validate_wiley_article_content()

    def valid_crossref_metadata(self, crossref_response: Response) -> bool:
        """Validate that a Crossref work record contains sufficient metadata.

        Args:
            crossref_response: A response from Crossref to be validated.
        """
        valid = False
        if work_record := crossref_response.json():
            if (
                work_record.get("message", {}).get("title") is not None
                and work_record.get("message", {}).get("URL") is not None
            ):
                valid = True
                logger.debug("Sufficient metadata downloaded for %s", self.doi)
            else:
                logger.exception(
                    "Insufficient metadata for %s, missing title or URL", self.doi
                )
        else:
            logger.exception("Unable to parse %s response as JSON", self.doi)
        return valid

    def get_and_validate_crossref_metadata(self) -> None:
        """Get and validate metadata from Crossref API."""
        crossref_response = get_response_from_doi(self.metadata_url, self.doi)
        if self.valid_crossref_metadata(crossref_response) is False:
            raise InvalidCrossrefMetadataError
        self.crossref_metadata = crossref_response.json()

    def create_dspace_metadata(self, metadata_mapping_path: str) -> dict[str, Any]:
        """Create DSpace metadata from Crossref metadata and a metadata mapping file.

        Args:
            metadata_mapping_path: Path to a JSON metadata mapping file
        """
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
        with open(metadata_mapping_path) as metadata_mapping_file:
            metadata_mapping = json.load(metadata_mapping_file)
            work = self.crossref_metadata["message"]
            metadata = []
            for key in [k for k in work if k in keys_for_dspace]:
                if key == "author":
                    metadata.extend(
                        [
                            {
                                "key": metadata_mapping[key],
                                "value": f'{author.get("family")}, {author.get("given")}',
                            }
                            for author in work["author"]
                        ]
                    )
                elif key == "title":
                    metadata.append(
                        {
                            "key": metadata_mapping[key],
                            "value": ". ".join(t for t in work[key]),
                        }
                    )
                elif key == "issued":
                    metadata.append(
                        {
                            "key": metadata_mapping[key],
                            "value": "-".join(
                                str(d).zfill(2) for d in work["issued"]["date-parts"][0]
                            ),
                        }
                    )
                elif isinstance(work[key], list):
                    metadata.extend(
                        [
                            {"key": metadata_mapping[key], "value": list_item}
                            for list_item in work[key]
                        ]
                    )
                else:
                    metadata.append({"key": metadata_mapping[key], "value": work[key]})
            return {"metadata": metadata}

    def valid_dspace_metadata(self, dspace_metadata: dict[str, Any]) -> bool:
        """Validate that the dspace_metadata attribute follows the expected format.

        Args:
            dspace_metadata: DSpace metadata to be validated.
        """
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
        valid = False
        if dspace_metadata.get("metadata") is not None:
            for element in dspace_metadata["metadata"]:
                if (
                    element.get("key") is not None
                    and element.get("value") is not None
                    and element.get("key") in approved_metadata_fields
                ):
                    valid = True
            logger.debug("Valid DSpace metadata created")
        else:
            logger.exception("Invalid DSpace metadata created: %s ", dspace_metadata)
        return valid

    def create_and_validate_dspace_metadata(self) -> None:
        """Create and validate DSpace metadata from Crossref metadata."""
        dspace_metadata = self.create_dspace_metadata(
            "config/metadata_mapping.json",
        )
        if self.valid_dspace_metadata(dspace_metadata) is False:
            raise InvalidDSpaceMetadataError
        self.dspace_metadata = dspace_metadata

    def valid_article_content_response(self, wiley_response: Response) -> bool:
        """Validate the Wiley response contained a PDF.

        Args:
           wiley_response: A response from the Wiley server to be validated.
        """
        valid = False
        if wiley_response.headers["content-type"] == "application/pdf; charset=UTF-8":
            valid = True
            logger.debug("PDF downloaded for %s", self.doi)
        else:
            logger.exception("A PDF could not be retrieved for DOI: %s", self.doi)
            logger.debug(
                "Response contents retrieved from Wiley server for %s: %s",
                self.doi,
                wiley_response.content,
            )
        return valid

    def get_and_validate_wiley_article_content(self) -> None:
        """Get and validate article content from the Wiley server."""
        wiley_response = get_wiley_response(self.content_url, self.doi)
        if self.valid_article_content_response(wiley_response) is False:
            raise InvalidArticleContentResponseError
        self.article_content = wiley_response.content


class InvalidCrossrefMetadataError(Exception):
    pass


class InvalidDSpaceMetadataError(Exception):
    pass


class InvalidArticleContentResponseError(Exception):
    pass
