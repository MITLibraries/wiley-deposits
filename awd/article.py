import json
import logging
from typing import Any

from awd.status import Status

logger = logging.getLogger(__name__)


class Article:
    """Article class.

    Class represents articles published by Wiley for which we will attempt to get
    metadata and binary content for uploading to our DSpace repository.
    """

    def __init__(self, doi: str) -> None:
        """Initialize article instance.

        Args:
            doi: A digital object identifer (doi) for an article.
        """
        self.doi: str = doi
        self.database_item = None
        self.crossref_metadata: dict[str, Any]
        self.dspace_metadata: dict[str, Any]
        self.article_content: bytes

    def exists_in_database(self, database_items: list[dict[str, Any]]) -> bool:
        """Validate that a DOI is NOT in the database and needs to be added.

        Args:
            database_items: A list of database items that may or may not contain the
            specified DOI.
        """
        exists = False
        if not any(doi_item["doi"] == self.doi for doi_item in database_items):
            exists = True
            logger.debug("%s added to database", self.doi)
        return exists

    def has_retry_status(self, database_items: list[dict[str, Any]]) -> bool:
        """Validate that a DOI should be retried based on its status in the database.

        Args:
            database_items: A list of database items containing the specified DOI, whose
            status must be evaluated for whether the application should attempt to process
            it again.
        """
        retry_status = False
        if any(
            d
            for d in database_items
            if d["doi"] == self.doi and d["status"] == str(Status.UNPROCESSED.value)
        ):
            retry_status = True
            logger.debug("%s will be retried", self.doi)
        return retry_status

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

    def valid_dspace_metadata(self) -> bool:
        """Validate that metadata follows the format expected by DSpace."""
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
        if self.dspace_metadata.get("metadata") is not None:
            for element in self.dspace_metadata["metadata"]:
                if (
                    element.get("key") is not None
                    and element.get("value") is not None
                    and element.get("key") in approved_metadata_fields
                ):
                    valid = True
            logger.debug("Valid DSpace metadata created")
        else:
            logger.exception("Invalid DSpace metadata created: %s ", self.dspace_metadata)
        return valid
