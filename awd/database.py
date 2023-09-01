from __future__ import annotations

import datetime
import logging
from typing import Any

from pynamodb.attributes import NumberAttribute, UnicodeAttribute
from pynamodb.models import Model

from awd.config import DATE_FORMAT
from awd.status import Status

logger = logging.getLogger(__name__)


class DoiProcessAttempt(Model):
    """A class modeling an item in the DynamoDB table."""

    class Meta:  # noqa: D106
        table_name = "None"

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

    def has_unprocessed_status(self) -> bool:
        """Validate that a DOI has unprocessed status in the DOI table."""
        return self.get(self.doi).status == Status.UNPROCESSED.value

    def attempts_exceeded(self, retry_threshold: int) -> bool:
        """Validate whether a DOI has exceeded the retry threshold.

        Args:
            retry_threshold: The number of attempts that should be
            made before setting the item to a failed status.
        """
        attempts_exceeded = False
        if self.attempts >= retry_threshold:
            attempts_exceeded = True
        return attempts_exceeded

    def increment_attempts(self) -> None:
        """Increment attempts for DOI item in DOI table."""
        self.attempts += 1
        self.last_modified = datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT)
        self.save()
        logger.debug("%s attempts updated to: %s", self.doi, self.attempts)

    @classmethod
    def set_table_name(cls, table_name: str) -> None:
        """Set table_name attribute.

        Args:
            table_name: The name of the DynamoDB table.
        """
        cls.Meta.table_name = table_name

    def update_status(self, status_code: int) -> None:
        """Update status for DOI item in DOI table.

        Args:
            status_code: The status code to be set for the item.
        """
        self.status = status_code
        self.last_modified = datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT)
        self.save()
        logger.debug("%s status updated to: %s", self.doi, self.status)
