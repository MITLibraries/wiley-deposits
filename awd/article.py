import logging
from typing import TYPE_CHECKING, Any

from awd.status import Status

if TYPE_CHECKING:
    from requests import Response

logger = logging.getLogger(__name__)


class Article:
    """Article class."""

    def __init__(self, doi: str) -> None:
        """Initialize article instance.

        Args:
            doi: A digital object identifer (doi) for an article.
        """
        self.doi: str = doi
        self.database_item = None
        self.raw_metadata_response: Response | None = None
        self.source_metadata: dict[str, Any] | None = None
        self.transformed_metadata: dict[str, Any] | None = None
        self.pdf: Response | None = None

    def doi_to_be_added(self, database_items: list[dict[str, Any]]) -> bool:
        """Validate that a DOI is in the database and needs to be added.

        Args:
            database_items: .

        Returns:
            validation_status: .
        """
        validation_status = False
        if not any(doi_item["doi"] == self.doi for doi_item in database_items):
            validation_status = True
            logger.debug("%s added to database", self.doi)
        return validation_status

    def doi_to_be_retried(self, database_items: list[dict[str, Any]]) -> bool:
        """Validate that a DOI should be retried based on its status in the database.

        Args:
            database_items: !!!!!

        Returns:
            validation_status: !!!!!
        """
        validation_status = False
        if any(
            d
            for d in database_items
            if d["doi"] == self.doi and d["status"] == str(Status.UNPROCESSED.value)
        ):
            validation_status = True
            logger.debug("%s will be retried", self.doi)
        return validation_status
