import logging

import boto3
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger(__name__)


class DynamoDB:
    """An DynamoDB class that provides a generic boto3 DynamoDB client."""

    def __init__(self):
        self.client = boto3.client(
            "dynamodb",
            region_name="us-east-1",
        )

    def add_doi_item_to_database(self, doi_table, doi):
        """Add DOI item to database table."""
        response = self.client.put_item(
            TableName=doi_table,
            Item={
                "doi": {"S": doi},
                "status": {"S": "Processing"},
                "attempts": {"S": "0"},
            },
        )
        return response

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
        response = self.client.put_item(TableName=doi_table, Item=item["Item"])
        return response

    def update_doi_item_status_in_database(
        self,
        doi_table,
        doi,
        status,
    ):
        """Update status for DOI item in database."""
        item = self.client.get_item(
            TableName=doi_table,
            Key={"doi": {"S": doi}},
        )
        item["Item"]["status"]["S"] = status
        response = self.client.put_item(
            TableName=doi_table,
            Item=item["Item"],
        )
        return response


def doi_to_be_added(doi, doi_items):
    "Validate that a DOI is not a part of the database table and needs to  be added."
    validation_status = False
    if not any(doi_item["doi"] == doi for doi_item in doi_items):
        validation_status = True
    return validation_status


def doi_to_be_retried(doi, doi_items):
    "Validate that a DOI should be retried based on its status in the database table."
    validation_status = False
    for doi_item in [
        d for d in doi_items if d["doi"] == doi and d["status"] == "Failed, will retry"
    ]:
        validation_status = True
        logger.debug(f"{doi} will be retried.")
    return validation_status
