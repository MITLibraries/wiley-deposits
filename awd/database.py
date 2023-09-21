from __future__ import annotations

import datetime
import logging
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
        table_name = "None"

    doi = UnicodeAttribute(hash_key=True)
    process_attempts = NumberAttribute()
    last_modified = UnicodeAttribute()
    status_code = NumberAttribute()

    @classmethod
    def add_item(cls, doi: str) -> dict[str, Any]:
        """Add DOI item to DOI table.

        Args:
            doi: The DOI to be added to the DOI table.
        """
        response = cls(
            doi=doi,
            process_attempts=0,
            last_modified=datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT),
            status_code=Status.UNPROCESSED.value,
        ).save()
        logger.debug("%s added to table", doi)
        return response

    @classmethod
    def check_doi_and_add_to_table(cls, doi: str) -> None:
        """Check if DOI should be added to table.

        If not present, add the DOI to the table.

        Args:
            doi: The DOI to be checked and possibly added to the DOI table.
        """
        try:
            cls.get(doi)
        except DoesNotExist:
            cls.add_item(doi)

    def has_unprocessed_status(self) -> bool:
        """Validate that a DOI has unprocessed status in the DOI table."""
        return self.get(self.doi).status_code == Status.UNPROCESSED.value

    def increment_process_attempts(self) -> None:
        """Increment process attempts for DOI item in DOI table."""
        self.process_attempts += 1
        self.last_modified = datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT)
        self.save()
        logger.debug(
            "%s process attempts updated to: %s", self.doi, self.process_attempts
        )

    def process_attempts_exceeded(self, retry_threshold: int) -> bool:
        """Validate whether a DOI has exceeded the retry threshold.

        Args:
            retry_threshold: The number of process attempts that should be
            made before setting the item to a failed status.
        """
        process_attempts_exceeded = False
        if self.process_attempts >= retry_threshold:
            process_attempts_exceeded = True
        return process_attempts_exceeded

    @classmethod
    def retrieve_unprocessed_dois(cls) -> list[str]:
        """Retrieve all unprocessed DOI items from database table."""
        return [
            item.doi
            for item in cls.scan()
            if item.status_code == Status.UNPROCESSED.value
        ]

    @classmethod
    def set_table_name(cls, table_name: str) -> None:
        """Set table_name attribute.

        The table name must be set dynamically rather than from an env variable
        due to the current configuration process.

        Args:
            table_name: The name of the DynamoDB table.
        """
        cls.Meta.table_name = table_name

    def sqs_error_update_status(self, retry_threshold: int) -> None:
        """Update status for error result message.

        Args:
            retry_threshold: The number of process attempts that should be
            made before setting the item to a failed status.
        """
        if self.process_attempts_exceeded(retry_threshold=retry_threshold):
            self.update_status(status_code=Status.FAILED.value)
            logger.exception(
                "DOI: '%s' has exceeded the retry threshold and will not be "
                "attempted again.",
                self.doi,
            )
        else:
            self.update_status(status_code=Status.UNPROCESSED.value)

    def update_status(self, status_code: int) -> None:
        """Update status for DOI item in DOI table.

        Args:
            status_code: The status code to be set for the item.
        """
        self.status_code = status_code
        self.last_modified = datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT)
        self.save()
        logger.debug("%s status updated to: %s", self.doi, self.status_code)
