from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING, Any, Iterator, Mapping

from boto3 import client

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import (
        EmptyResponseMetadataTypeDef,
        MessageAttributeValueTypeDef,
        MessageTypeDef,
        SendMessageResultTypeDef,
    )

logger = logging.getLogger(__name__)


class SQS:
    """An SQS class that provides a generic boto3 SQS client."""

    def __init__(self, region: str) -> None:
        self.client = client("sqs", region_name=region)

    def delete(
        self, sqs_base_url: str, queue_name: str, receipt_handle: str
    ) -> EmptyResponseMetadataTypeDef:
        """Delete message from SQS queue."""
        logger.debug("Deleting %s from SQS queue: %s", receipt_handle, queue_name)
        response = self.client.delete_message(
            QueueUrl=f"{sqs_base_url}{queue_name}",
            ReceiptHandle=receipt_handle,
        )
        logger.debug("Message deleted from SQS queue: %s", response)
        return response

    def send(
        self,
        sqs_base_url: str,
        queue_name: str,
        message_attributes: Mapping[str, MessageAttributeValueTypeDef],
        message_body: str,
    ) -> SendMessageResultTypeDef:
        """Send message via SQS."""
        logger.debug("Sending message to SQS queue: %s", queue_name)
        response = self.client.send_message(
            QueueUrl=f"{sqs_base_url}{queue_name}",
            MessageAttributes=message_attributes,
            MessageBody=str(message_body),
        )
        logger.debug("Response from SQS queue: %s", response)
        return response

    def receive(
        self,
        sqs_base_url: str,
        queue_name: str,
    ) -> Iterator[MessageTypeDef]:
        """Receive message via SQS."""
        logger.debug("Receiving messages from SQS queue: %s", queue_name)
        while True:
            response = self.client.receive_message(
                QueueUrl=f"{sqs_base_url}{queue_name}",
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
            )
            if "Messages" in response:
                for message in response["Messages"]:
                    logger.debug(
                        "Message retrieved from SQS queue %s: %s", queue_name, message
                    )
                    yield message
            else:
                logger.debug("No more messages from SQS queue: %s", queue_name)
                break


def create_dss_message_attributes(
    package_id: str, submission_source: str, output_queue: str
) -> dict[str, Any]:
    """Create attributes for a DSpace Submission Service message."""
    return {
        "PackageID": {"DataType": "String", "StringValue": package_id},
        "SubmissionSource": {"DataType": "String", "StringValue": submission_source},
        "OutputQueue": {"DataType": "String", "StringValue": output_queue},
    }


def create_dss_message_body(
    submission_system: str,
    collection_handle: str,
    metadata_s3_uri: str,
    bitstream_name: str,
    bitstream_s3_uri: str,
) -> str:
    """Create body for a DSpace Submission Service message."""
    return json.dumps(
        {
            "SubmissionSystem": submission_system,
            "CollectionHandle": collection_handle,
            "MetadataLocation": metadata_s3_uri,
            "Files": [
                {
                    "BitstreamName": bitstream_name,
                    "FileLocation": bitstream_s3_uri,
                    "BitstreamDescription": None,
                }
            ],
        }
    )
