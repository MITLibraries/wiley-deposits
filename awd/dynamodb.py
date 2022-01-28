import logging
from datetime import datetime

import boto3
from boto3.dynamodb.types import TypeDeserializer

from awd.status import Status

logger = logging.getLogger(__name__)


class DynamoDB:
    """An DynamoDB class that provides a generic boto3 DynamoDB client."""

    def __init__(self, region):
        self.client = boto3.client(
            "dynamodb",
            region_name=region,
        )

    def add_doi_item_to_database(self, doi_table, doi):
        """Add DOI item to database table."""
        response = self.client.put_item(
            TableName=doi_table,
            Item={
                "doi": {"S": doi},
                "status": {"S": str(Status.PROCESSING.value)},
                "attempts": {"S": "0"},
                "last_modified": {"S": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
            },
        )
        logger.debug(f"{doi} added to table")
        return response

    def check_read_permissions(self, doi_table):
        """Verify that the contents of the specified table can be read."""
        self.client.scan(
            TableName=doi_table,
        )
        logger.debug(f"Able to access table: {doi_table}")
        return f"Read permissions confirmed for table: {doi_table}"

    def check_write_permissions(self, doi_table):
        """Verify that items can be written to the specified table."""
        self.add_doi_item_to_database(doi_table, "SmokeTest")
        if "Item" in self.client.get_item(
            TableName=doi_table,
            Key={"doi": {"S": "SmokeTest"}},
        ):
            logger.debug(f"Test item successfully written to table: {doi_table}")
        else:
            logger.error(f"Unable to write item to table: {doi_table}")
        self.client.delete_item(
            TableName=doi_table,
            Key={"doi": {"S": "SmokeTest"}},
        )
        logger.debug(f"Test item deleted from table: {doi_table}")
        return f"Write permissions confirmed for table: {doi_table}"

    def retrieve_doi_items_from_database(self, doi_table):
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

    def retry_attempts_exceeded(self, doi_table, doi, retry_threshold):
        """Validate whether a DOI has exceeded the retry threshold."""
        validation_status = False
        item = self.client.get_item(
            TableName=doi_table,
            Key={"doi": {"S": doi}},
        )
        if int(item["Item"]["attempts"]["S"]) >= int(retry_threshold):
            validation_status = True
        return validation_status

    def update_doi_item_attempts_in_database(self, doi_table, doi):
        """Increment attempts for  DOI item in database."""
        item = self.client.get_item(
            TableName=doi_table,
            Key={"doi": {"S": doi}},
        )
        item["Item"]["attempts"]["S"] = str(int(item["Item"]["attempts"]["S"]) + 1)
        item["Item"]["last_modified"]["S"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        response = self.client.put_item(TableName=doi_table, Item=item["Item"])
        logger.debug(
            f'{doi} attempts updated to: {str(int(item["Item"]["attempts"]["S"]) + 1)}'
        )
        return response

    def update_doi_item_status_in_database(
        self,
        doi_table,
        doi,
        status_code,
    ):
        """Update status for DOI item in database."""
        item = self.client.get_item(
            TableName=doi_table,
            Key={"doi": {"S": doi}},
        )
        item["Item"]["status"]["S"] = str(status_code)
        item["Item"]["last_modified"]["S"] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        response = self.client.put_item(
            TableName=doi_table,
            Item=item["Item"],
        )
        logger.debug(f"{doi} status updated to: {status_code}")
        return response
