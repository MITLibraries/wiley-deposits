import logging
from datetime import datetime
from typing import Any

import boto3
from boto3.dynamodb.types import TypeDeserializer
from mypy_boto3_dynamodb.type_defs import PutItemOutputTypeDef

from awd.status import Status

logger = logging.getLogger(__name__)

date_format = "%Y-%m-%d %H:%M:%S"


class DynamoDB:
    """An DynamoDB class that provides a generic boto3 DynamoDB client."""

    def __init__(self, region: str) -> None:
        self.client = boto3.client(
            "dynamodb",
            region_name=region,
        )

    def add_doi_item_to_database(
        self, doi_table: str, doi: str
    ) -> PutItemOutputTypeDef:
        """Add DOI item to database table."""
        response = self.client.put_item(
            TableName=doi_table,
            Item={
                "doi": {"S": doi},
                "status": {"S": str(Status.UNPROCESSED.value)},
                "attempts": {"S": "0"},
                "last_modified": {"S": datetime.now().strftime(date_format)},
            },
        )
        logger.debug(f"{doi} added to table")
        return response

    def retrieve_doi_items_from_database(self, doi_table: str) -> list[dict[str, Any]]:
        """Retrieve all DOI items from database table."""
        response = self.client.scan(
            TableName=doi_table,
        )
        doi_items = []
        for item in response["Items"]:
            deserializer = TypeDeserializer()
            deserialized_item = {
                k: deserializer.deserialize(v) for k, v in item.items()
            }
            doi_items.append(deserialized_item)
        return doi_items

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

    def update_doi_item_attempts_in_database(
        self, doi_table: str, doi: str
    ) -> PutItemOutputTypeDef:
        """Increment attempts for  DOI item in database."""
        item = self.client.get_item(
            TableName=doi_table,
            Key={"doi": {"S": doi}},
        )
        logger.debug("Response retrieved from DynamoDB table: %s", item)
        item["Item"]["attempts"]["S"] = str(int(item["Item"]["attempts"]["S"]) + 1)
        item["Item"]["last_modified"]["S"] = datetime.now().strftime(date_format)
        response = self.client.put_item(TableName=doi_table, Item=item["Item"])
        logger.debug(
            f"{doi} attempts updated to: "
            f'{str(int(item["Item"]["attempts"]["S"]) + 1)}'
        )
        return response

    def update_doi_item_status_in_database(
        self,
        doi_table: str,
        doi: str,
        status_code: int,
    ) -> PutItemOutputTypeDef:
        """Update status for DOI item in database."""
        item = self.client.get_item(
            TableName=doi_table,
            Key={"doi": {"S": doi}},
        )
        logger.debug("Response retrieved from DynamoDB table: %s", item)
        item["Item"]["status"]["S"] = str(status_code)
        item["Item"]["last_modified"]["S"] = datetime.now().strftime(date_format)
        response = self.client.put_item(
            TableName=doi_table,
            Item=item["Item"],
        )
        logger.debug(f"{doi} status updated to: {status_code}")
        return response
