from __future__ import annotations

import datetime
import logging
from typing import Any

from pynamodb.attributes import NumberAttribute, UnicodeAttribute
from pynamodb.models import Model

from awd.config import DATE_FORMAT
from awd.status import Status

logger = logging.getLogger(__name__)


class DoiTable(Model):
    """A class modeling the DynamoDB table."""

    class Meta:  # noqa: D106
        table_name = "None"

    doi = UnicodeAttribute(hash_key=True)
    attempts = NumberAttribute()
    last_modified = UnicodeAttribute()
    status = NumberAttribute()

    def add_item(self, doi: str) -> dict[str, Any]:
        """Add DOI item to DOI table.

        Args:
            doi: The DOI to be added to the DOI table.
        """
        response = DoiTable(
            doi=doi,
            attempts=0,
            last_modified=datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT),
            status=Status.UNPROCESSED.value,
        ).save()
        logger.debug("%s added to table", doi)
        return response

    def attempts_exceeded(self, doi: str, retry_threshold: int) -> bool:
        """Validate whether a DOI has exceeded the retry threshold.

        Args:
            doi: The DOI to be checked.
            retry_threshold: The number of attempts that should be
            made before setting the item to a failed status.
        """
        attempts_exceeded = False
        item = DoiTable.get(doi)
        if item.attempts >= retry_threshold:
            attempts_exceeded = True
        return attempts_exceeded

    def increment_attempts(self, doi: str) -> None:
        """Increment attempts for  DOI item in DOI table.

        Args:
            doi: The DOI item to be updated in the DOI table.
        """
        item = DoiTable.get(doi)
        logger.debug("Response retrieved from DynamoDB table: %s", item)
        updated_attempts = item.attempts + 1
        response = item.update(
            actions=[
                DoiTable.attempts.set(updated_attempts),
                DoiTable.last_modified.set(
                    datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT)
                ),
            ]
        )
        logger.debug("%s attempts updated to: %s", doi, updated_attempts)
        return response

    def retrieve_items(self) -> list[DoiTable]:
        """Retrieve DOI table items as a list."""
        return list(self.scan())

    def set_table_name(self, table_name: str) -> None:
        """Set table_name attribute.

        Args:
            table_name: The name of the DynamoDB table.
        """
        self.Meta.table_name = table_name

    def update_status(self, doi: str, status_code: int) -> None:
        """Update status for DOI item in DOI table.

        Args:
            doi: The DOI to be updated in the DOI table.
            status_code: The status code to be set for the item.
        """
        item = DoiTable.get(doi)
        logger.debug("Response retrieved from DynamoDB table: %s", item)
        response = item.update(
            actions=[
                DoiTable.status.set(status_code),
                DoiTable.last_modified.set(
                    datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT)
                ),
            ]
        )
        logger.debug("%s status updated to: %s", doi, status_code)
        return response
