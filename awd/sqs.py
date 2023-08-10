import json
import logging
from typing import Any, Iterator, Mapping

from boto3 import client
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
        logger.debug(f"Deleting {receipt_handle} from SQS queue: {queue_name}")
        response = self.client.delete_message(
            QueueUrl=f"{sqs_base_url}{queue_name}",
            ReceiptHandle=receipt_handle,
        )
        logger.debug(f"Message deleted from SQS queue: {response}")
        return response

    def send(
        self,
        sqs_base_url: str,
        queue_name: str,
        message_attributes: Mapping[str, MessageAttributeValueTypeDef],
        message_body: str,
    ) -> SendMessageResultTypeDef:
        """Send message via SQS."""
        logger.debug(f"Sending message to SQS queue: {queue_name}")
        response = self.client.send_message(
            QueueUrl=f"{sqs_base_url}{queue_name}",
            MessageAttributes=message_attributes,
            MessageBody=str(message_body),
        )
        logger.debug(f"Response from SQS queue: {response}")
        return response

    def receive(
        self,
        sqs_base_url: str,
        queue_name: str,
    ) -> Iterator[MessageTypeDef]:
        """Receive message via SQS."""
        logger.debug(f"Receiving messages from SQS queue: {queue_name}")
        while True:
            response = self.client.receive_message(
                QueueUrl=f"{sqs_base_url}{queue_name}",
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
            )
            if "Messages" in response:
                for message in response["Messages"]:
                    logger.debug(
                        f"Message retrieved from SQS queue {queue_name}: {message}"
                    )
                    yield message
            else:
                logger.debug(f"No more messages from SQS queue: {queue_name}")
                break


def create_dss_message_attributes(
    package_id: str, submission_source: str, output_queue: str
) -> dict[str, Any]:
    """Create attributes for a DSpace Submission Service message."""
    dss_message_attributes = {
        "PackageID": {"DataType": "String", "StringValue": package_id},
        "SubmissionSource": {"DataType": "String", "StringValue": submission_source},
        "OutputQueue": {"DataType": "String", "StringValue": output_queue},
    }
    return dss_message_attributes


def create_dss_message_body(
    submission_system: str,
    collection_handle: str,
    metadata_s3_uri: str,
    bitstream_name: str,
    bitstream_s3_uri: str,
) -> str:
    """Create body for a DSpace Submission Service message."""
    dss_message_body = {
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
    return json.dumps(dss_message_body)
