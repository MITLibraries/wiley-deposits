from __future__ import annotations

import datetime
import logging
import os
from typing import Any

from pynamodb.attributes import NumberAttribute, UnicodeAttribute
from pynamodb.exceptions import DoesNotExist
from pynamodb.models import Model

from awd.config import DATE_FORMAT
from awd.status import Status

logger = logging.getLogger(__name__)


class DoiProcessAttempt(Model):
    """A class modeling an item in the DynamoDB table."""

    class Meta:  # noqa: D106
        table_name = f"wiley-{os.getenv('WORKSPACE')}"

    doi = UnicodeAttribute(hash_key=True)
    attempts = NumberAttribute()
    last_modified = UnicodeAttribute()
    status = NumberAttribute()

    @classmethod
    def add_item(cls, doi: str) -> dict[str, Any]:
        """Add DOI item to DOI table.

        Args:
            doi: The DOI to be added to the DOI table.
        """
        response = cls(
            doi=doi,
            attempts=0,
            last_modified=datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT),
            status=Status.UNPROCESSED.value,
        ).save()
        logger.debug("%s added to table", doi)
        return response

    def has_unprocessed_status(self, doi: str) -> bool:
        """Validate that a DOI has unprocessed status in the DOI table."""
        return DoiProcessAttempt.get(doi).status == Status.UNPROCESSED.value

    def check_doi_and_add_to_table(self, doi: str) -> None:
        """Check if DOI should be added to table.

        If not present, add the DOI to the table.  Check for unprocessed status
        and raise exception if DOI should not be retried. Increment attempts field.

        Args:
            doi: The DOI to be checked and possibly added to the DOI table.
        """
        try:
            self.get(doi)
        except DoesNotExist:
            self.add_item(doi)
        if not self.has_unprocessed_status(doi):
            raise UnprocessedStatusFalseError
        self.increment_attempts(doi)

    @classmethod
    def attempts_exceeded(cls, doi: str, retry_threshold: int) -> bool:
        """Validate whether a DOI has exceeded the retry threshold.

        Args:
            doi: The DOI to be checked.
            retry_threshold: The number of attempts that should be
            made before setting the item to a failed status.
        """
        attempts_exceeded = False
        item = cls.get(doi)
        if item.attempts >= retry_threshold:
            attempts_exceeded = True
        return attempts_exceeded

    @classmethod
    def increment_attempts(cls, doi: str) -> None:
        """Increment attempts for  DOI item in DOI table.

        Args:
            doi: The DOI item to be updated in the DOI table.
        """
        item = cls.get(doi)
        logger.debug("Response retrieved from DynamoDB table: %s", item)
        updated_attempts = item.attempts + 1
        response = item.update(
            actions=[
                cls.attempts.set(updated_attempts),
                cls.last_modified.set(
                    datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT)
                ),
            ]
        )
        logger.debug("%s attempts updated to: %s", doi, updated_attempts)
        return response

    @classmethod
    def update_status(cls, doi: str, status_code: int) -> None:
        """Update status for DOI item in DOI table.

        Args:
            doi: The DOI to be updated in the DOI table.
            status_code: The status code to be set for the item.
        """
        item = cls.get(doi)
        logger.debug("Response retrieved from DynamoDB table: %s", item)
        response = item.update(
            actions=[
                cls.status.set(status_code),
                cls.last_modified.set(
                    datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT)
                ),
            ]
        )
        logger.debug("%s status updated to: %s", doi, status_code)
        return response


class UnprocessedStatusFalseError(Exception):
    pass
