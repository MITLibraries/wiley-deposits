from __future__ import annotations

import datetime
import logging
from typing import TYPE_CHECKING, Any

import boto3
from boto3.dynamodb.types import TypeDeserializer
from botocore.exceptions import ClientError

from awd.config import DATE_FORMAT
from awd.status import Status

if TYPE_CHECKING:
    from mypy_boto3_dynamodb.type_defs import PutItemOutputTypeDef

logger = logging.getLogger(__name__)


class DynamoDB:
    """An DynamoDB class that provides a generic boto3 DynamoDB client."""

    def __init__(self, region: str) -> None:
        self.client = boto3.client(
            "dynamodb",
            region_name=region,
        )

    def add_item_to_database(
        self, doi_table: str, doi: str
    ) -> PutItemOutputTypeDef | None:
        """Add DOI item to database table."""
        try:
            response = self.client.put_item(
                TableName=doi_table,
                Item={
                    "doi": {"S": doi},
                    "status": {"S": str(Status.UNPROCESSED.value)},
                    "attempts": {"S": "0"},
                    "last_modified": {
                        "S": datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT)
                    },
                },
            )
            logger.debug("%s added to table", doi)
        except ClientError as e:
            logger.exception(
                "Table error while processing %s: %s",
                doi,
                e.response["Error"]["Message"],
            )
            return None
        else:
            return response

    def retrieve_items_from_database(self, doi_table: str) -> list[dict[str, Any]] | None:
        """Retrieve all DOI items from database table."""
        try:
            response = self.client.scan(
                TableName=doi_table,
            )
            database_items = []
            for item in response["Items"]:
                deserializer = TypeDeserializer()
                deserialized_item = {
                    k: deserializer.deserialize(v) for k, v in item.items()
                }
                database_items.append(deserialized_item)
        except ClientError as e:
            logger.exception("Table read failed: %s", e.response["Error"]["Message"])
            return None
        else:
            return database_items

    def attempts_exceeded(self, doi_table: str, doi: str, retry_threshold: int) -> bool:
        """Validate whether a DOI has exceeded the retry threshold."""
        validation_status = False
        item = self.client.get_item(
            TableName=doi_table,
            Key={"doi": {"S": doi}},
        )
        if int(item["Item"]["attempts"]["S"]) >= int(retry_threshold):
            validation_status = True
        return validation_status

    def update_item_attempts_in_database(
        self, doi_table: str, doi: str
    ) -> PutItemOutputTypeDef:
        """Increment attempts for  DOI item in database."""
        item = self.client.get_item(
            TableName=doi_table,
            Key={"doi": {"S": doi}},
        )
        logger.debug("Response retrieved from DynamoDB table: %s", item)
        item["Item"]["attempts"]["S"] = str(int(item["Item"]["attempts"]["S"]) + 1)
        item["Item"]["last_modified"]["S"] = datetime.datetime.now(
            tz=datetime.UTC
        ).strftime(DATE_FORMAT)
        response = self.client.put_item(TableName=doi_table, Item=item["Item"])
        logger.debug(
            "%s attempts updated to: %s",
            doi,
            str(int(item["Item"]["attempts"]["S"]) + 1),
        )
        return response

    def update_item_status_in_database(
        self,
        doi_table: str,
        doi: str,
        status_code: int,
    ) -> PutItemOutputTypeDef | None:
        """Update status for DOI item in database."""
        try:
            item = self.client.get_item(
                TableName=doi_table,
                Key={"doi": {"S": doi}},
            )
            logger.debug("Response retrieved from DynamoDB table: %s", item)
            item["Item"]["status"]["S"] = str(status_code)
            item["Item"]["last_modified"]["S"] = datetime.datetime.now(
                tz=datetime.UTC
            ).strftime(DATE_FORMAT)
            response = self.client.put_item(
                TableName=doi_table,
                Item=item["Item"],
            )
            logger.debug("%s status updated to: %s", doi, status_code)
        except KeyError:
            logger.exception("Key error in table while processing %s", doi)
            return None
        except ClientError as e:
            logger.exception(
                "Table error while processing %s: %s",
                doi,
                e.response["Error"]["Message"],
            )
            return None
        else:
            return response
