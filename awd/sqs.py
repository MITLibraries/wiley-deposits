import logging

from boto3 import client

logger = logging.getLogger(__name__)


class SQS:
    """An SQS class that provides a generic boto3 SQS client."""

    def __init__(self):
        self.client = client("sqs", region_name="us-east-1")

    def delete(self, queue_url, receipt_handle):
        """Delete message from SQS queue."""
        logger.debug(f"Deleting {receipt_handle} from SQS queue: {queue_url}")
        response = self.client.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )
        logger.debug(f"Message deleted from SQS queue: {response}")
        return response

    def send(self, queue_url, message_attributes, message_body):
        """Send message via SQS."""
        logger.debug(f"Sending message to SQS queue: {queue_url}")
        response = self.client.send_message(
            QueueUrl=queue_url,
            MessageAttributes=message_attributes,
            MessageBody=str(message_body),
        )
        logger.debug(f"Response from SQS queue: {response}")
        return response

    def receive(self, queue_url):
        """Receive message via SQS."""
        logger.debug(f"Receiving messages from SQS queue: {queue_url}")
        while True:
            response = self.client.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=10
            )
            if "Messages" in response:
                for message in response["Messages"]:
                    logger.debug(
                        f"Message retrieved from SQS queue {queue_url}: {message}"
                    )
                    yield message
            else:
                logger.debug(f"No more messages from SQS queue {queue_url}")
                break


def create_dss_message_attributes(package_id, submission_source, output_queue):
    """Create attributes for a DSpace Submission Service message."""
    dss_message_attributes = {
        "PackageID": {"DataType": "String", "StringValue": package_id},
        "SubmissionSource": {"DataType": "String", "StringValue": submission_source},
        "OutputQueue": {"DataType": "String", "StringValue": output_queue},
    }
    return dss_message_attributes


def create_dss_message_body(
    submission_system,
    collection_handle,
    metadata_s3_uri,
    bitstream_name,
    bitstream_s3_uri,
):
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
    return dss_message_body
