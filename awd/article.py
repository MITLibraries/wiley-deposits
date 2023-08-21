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
        self.crossref_metadata: dict[str, Any] | None = None
        self.dspace_metadata: dict[str, Any] | None = None
        self.article_content: bytes | None = None

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
