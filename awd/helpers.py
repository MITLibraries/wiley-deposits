from __future__ import annotations

import json
import logging
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from typing import TYPE_CHECKING, Any

import requests
import smart_open
from boto3 import client

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

    from mypy_boto3_s3.type_defs import PutObjectOutputTypeDef
    from mypy_boto3_ses.type_defs import SendRawEmailResponseTypeDef
    from mypy_boto3_sqs.type_defs import (
        EmptyResponseMetadataTypeDef,
        MessageAttributeValueTypeDef,
        MessageTypeDef,
        SendMessageResultTypeDef,
    )


logger = logging.getLogger(__name__)

WILEY_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
}


class S3:
    """An S3 class that provides a generic boto3 s3 client.

    Includes specific S3 functionality necessary for Wiley deposits.
    """

    def __init__(self) -> None:
        self.client = client("s3")

    def filter_files_in_bucket(
        self, bucket: str, file_type: str, excluded_prefix: str
    ) -> Iterator[str]:
        """Retrieve file based on file extension, bucket, and without excluded prefix."""
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket)
        for s3_object in [
            s3_object
            for page in pages
            for s3_object in page["Contents"]
            if s3_object["Key"].endswith(file_type)
            and excluded_prefix not in s3_object["Key"]
        ]:
            yield s3_object["Key"]

    def archive_file_with_new_key(
        self, bucket: str, key: str, archived_key_prefix: str
    ) -> None:
        """Update the key of the specified file to archive it from processing."""
        self.client.copy_object(
            Bucket=bucket,
            CopySource=f"{bucket}/{key}",
            Key=f"{archived_key_prefix}/{key}",
        )
        self.client.delete_object(
            Bucket=bucket,
            Key=key,
        )

    def put_file(
        self, file: str | bytes, bucket: str, key: str
    ) -> PutObjectOutputTypeDef:
        """Put a file in a specified S3 bucket with a specified key."""
        response = self.client.put_object(
            Body=file,
            Bucket=bucket,
            Key=key,
        )
        logger.debug("%s uploaded to S3", key)
        return response


class SES:
    """An SES class that provides a generic boto3 SES client."""

    def __init__(self, region: str) -> None:
        self.client = client("ses", region_name=region)

    def create_email(
        self,
        subject: str,
        attachment_content: str,
        attachment_name: str,
    ) -> MIMEMultipart:
        """Create an email."""
        message = MIMEMultipart()
        message["Subject"] = subject
        attachment_object = MIMEApplication(attachment_content)
        attachment_object.add_header(
            "Content-Disposition", "attachment", filename=attachment_name
        )
        message.attach(attachment_object)
        return message

    def send_email(
        self, source_email: str, recipient_email_address: str, message: MIMEMultipart
    ) -> SendRawEmailResponseTypeDef:
        """Send email via SES."""
        return self.client.send_raw_email(
            Source=source_email,
            Destinations=[recipient_email_address],
            RawMessage={
                "Data": message.as_string(),
            },
        )


class SQS:
    """An SQS class that provides a generic boto3 SQS client."""

    def __init__(self, region: str) -> None:
        self.client = client("sqs", region_name=region)

    @staticmethod
    def create_dss_message_attributes(
        package_id: str, submission_source: str, output_queue: str
    ) -> dict[str, Any]:
        """Create attributes for a DSpace Submission Service message."""
        return {
            "PackageID": {"DataType": "String", "StringValue": package_id},
            "SubmissionSource": {"DataType": "String", "StringValue": submission_source},
            "OutputQueue": {"DataType": "String", "StringValue": output_queue},
        }

    @staticmethod
    def create_dss_message_body(
        submission_system: str,
        collection_handle: str,
        metadata_s3_uri: str,
        bitstream_file_name: str,
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
                        "BitstreamName": bitstream_file_name,
                        "FileLocation": bitstream_s3_uri,
                        "BitstreamDescription": None,
                    }
                ],
            }
        )

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


def get_dois_from_spreadsheet(file: str) -> Iterator[str]:
    """Retriev DOIs from the Wiley-provided CSV file."""
    with smart_open.open(file, encoding="utf-8-sig") as csvfile:
        yield from csvfile.read().splitlines()


def get_crossref_response_from_doi(url: str, doi: str) -> requests.Response:
    """Retrieve Crossref response containing work record based on a DOI."""
    logger.debug("Requesting metadata for %s%s", url, doi)
    response = requests.get(
        f"{url}{doi}",
        params={
            "mailto": "dspace-lib@mit.edu",
        },
        timeout=30,
    )
    logger.debug("Response code retrieved from Crossref for %s: %s", doi, response)
    return response


def get_wiley_response(url: str, doi: str) -> requests.Response:
    """Get response from Wiley server based on a DOI."""
    logger.debug("Requesting PDF for %s%s", url, doi)
    response = requests.get(f"{url}{doi}", headers=WILEY_HEADERS, timeout=30)
    logger.debug("Response code retrieved from Wiley server for %s: %s", doi, response)
    return response
