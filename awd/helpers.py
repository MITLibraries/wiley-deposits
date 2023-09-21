from __future__ import annotations

import json
import logging
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from typing import TYPE_CHECKING, Any

import requests
import smart_open
from boto3 import client

from awd.database import DoiProcessAttempt
from awd.status import Status

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping
    from io import StringIO

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


class S3Client:
    """An S3 class that provides a generic boto3 s3 client.

    Includes specific S3 functionality necessary for Wiley deposits.
    """

    def __init__(self) -> None:
        self.client = client("s3")

    def archive_file_with_new_key(
        self, bucket: str, key: str, archived_key_prefix: str
    ) -> None:
        """Update the key of the specified file to archive it from processing.

        Args:
            bucket: The S3 bucket containing the files to be archived.
            key: The key of the file to archive.
            archived_key_prefix: The prefix to be applied to the archived file.
        """
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
        self, file_content: str | bytes, bucket: str, key: str
    ) -> PutObjectOutputTypeDef:
        """Put a file in a specified S3 bucket with a specified key.

        Args:
            file_content: The content of the file to be uploaded.
            bucket: The S3 bucket where the file will be uploaded.
            key: The key to be used for the uploaded file.
        """
        response = self.client.put_object(
            Body=file_content,
            Bucket=bucket,
            Key=key,
        )
        logger.debug("%s uploaded to S3", key)
        return response

    def retrieve_file_type_from_bucket(
        self, bucket: str, file_type: str, excluded_key_prefix: str
    ) -> Iterator[str]:
        """Retrieve file based on file type, bucket, and without excluded prefix.

        Args:
            bucket: The S3 bucket to search.
            file_type: The file type to retrieve.
            excluded_key_prefix: Files with this key prefix will not be retrieved.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket)
        for s3_object in [
            s3_object
            for page in pages
            for s3_object in page["Contents"]
            if s3_object["Key"].endswith(file_type)
            and excluded_key_prefix not in s3_object["Key"]
        ]:
            yield s3_object["Key"]


class SESClient:
    """An SES class that provides a generic boto3 SES client."""

    def __init__(self, region: str) -> None:
        self.client = client("ses", region_name=region)

    def create_email(
        self,
        subject: str,
        attachment_content: str,
        attachment_name: str,
    ) -> MIMEMultipart:
        """Create an email.

        Args:
            subject: The subject of the email.
            attachment_content: The content of the email attachment.
            attachment_name: The name of the email attachment.
        """
        message = MIMEMultipart()
        message["Subject"] = subject
        attachment_object = MIMEApplication(attachment_content)
        attachment_object.add_header(
            "Content-Disposition", "attachment", filename=attachment_name
        )
        message.attach(attachment_object)
        return message

    def send_email(
        self,
        source_email_address: str,
        recipient_email_address: str,
        message: MIMEMultipart,
    ) -> SendRawEmailResponseTypeDef:
        """Send email via SES.

        Args:
            source_email_address: The email address of the sender.
            recipient_email_address: The email address of the receipient.
            message: The message to be sent.
        """
        return self.client.send_raw_email(
            Source=source_email_address,
            Destinations=[recipient_email_address],
            RawMessage={
                "Data": message.as_string(),
            },
        )

    def create_and_send_email(
        self,
        subject: str,
        attachment_content: str,
        attachment_name: str,
        source_email_address: str,
        recipient_email_address: str,
    ) -> None:
        """Create an email message and send it via SES.

        Args:
           subject: The subject of the email.
           attachment_content: The content of the email attachment.
           attachment_name: The name of the email attachment.
           source_email_address: The email address of the sender.
           recipient_email_address: The email address of the receipient.
        """
        message = self.create_email(subject, attachment_content, attachment_name)
        self.send_email(source_email_address, recipient_email_address, message)
        logger.debug("Logs sent to %s", recipient_email_address)


class SQSClient:
    """An SQS class that provides a generic boto3 SQS client."""

    def __init__(self, region: str, base_url: str, queue_name: str) -> None:
        self.client = client("sqs", region_name=region)
        self.base_url: str = base_url
        self.queue_name: str = queue_name

    @staticmethod
    def create_dss_message_attributes(
        package_id: str, submission_source: str, output_queue: str
    ) -> dict[str, Any]:
        """Create attributes for a DSpace Submission Service message.

        Args:
            package_id: The PackageID field which is populated by the DOI.
            submission_source: The submission source, "wiley" for this application.
            output_queue: The SQS output queue used for retrieving result messages.
        """
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
        """Create body for a DSpace Submission Service message.

        Args:
        submission_system: The system where the article is uploaded.
        collection_handle: The handle of collection where the article is uploaded.
        metadata_s3_uri: The S3 URI for the metadata JSON file.
        bitstream_file_name: The file name for the article content which is uploaded as a
        bitstream.
        bitstream_s3_uri: The S3 URI for the article content file.
        """
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

    def delete(self, receipt_handle: str) -> EmptyResponseMetadataTypeDef:
        """Delete message from SQS queue.

        Args:
            receipt_handle: The receipt handle of the message to be deleted.
        """
        logger.debug("Deleting %s from SQS queue: %s", receipt_handle, self.queue_name)
        response = self.client.delete_message(
            QueueUrl=f"{self.base_url}{self.queue_name}",
            ReceiptHandle=receipt_handle,
        )
        logger.debug("Message deleted from SQS queue: %s", response)
        return response

    def process_result_message(
        self,
        sqs_message: MessageTypeDef,
        retry_threshold: str,
    ) -> None:
        """Validate and then process an SQS result message based on content.

        Args:
            sqs_message: An SQS result message to be processed.
            retry_threshold: The number of times to attempt processing an article.
        """
        if not self.valid_sqs_message(sqs_message):
            raise InvalidSQSMessageError
        doi = sqs_message["MessageAttributes"]["PackageID"]["StringValue"]
        doi_process_attempt = DoiProcessAttempt.get(doi)

        message_body = json.loads(str(sqs_message["Body"]))
        receipt_handle = sqs_message["ReceiptHandle"]
        self.delete(receipt_handle)
        logger.exception("DOI: %s, Result: %s", doi, message_body)

        if message_body["ResultType"] == "error":
            doi_process_attempt.sqs_error_update_status(int(retry_threshold))
        else:
            doi_process_attempt.update_status(status_code=Status.SUCCESS.value)

    def receive(self) -> Iterator[MessageTypeDef]:
        """Receive messages from SQS queue."""
        logger.debug("Receiving messages from SQS queue: %s", self.queue_name)
        while True:
            response = self.client.receive_message(
                QueueUrl=f"{self.base_url}{self.queue_name}",
                MaxNumberOfMessages=10,
                MessageAttributeNames=["All"],
            )
            if "Messages" in response:
                for message in response["Messages"]:
                    logger.debug(
                        "Message retrieved from SQS queue %s: %s",
                        self.queue_name,
                        message,
                    )
                    yield message
            else:
                logger.debug("No more messages from SQS queue: %s", self.queue_name)
                break

    def send(
        self,
        message_attributes: Mapping[str, MessageAttributeValueTypeDef],
        message_body: str,
    ) -> SendMessageResultTypeDef:
        """Send message via SQS.

        Args:
            message_attributes: The attributes of the message to send.
            message_body: The body of the message to send.
        """
        logger.debug("Sending message to SQS queue: %s", self.queue_name)
        response = self.client.send_message(
            QueueUrl=f"{self.base_url}{self.queue_name}",
            MessageAttributes=message_attributes,
            MessageBody=str(message_body),
        )
        logger.debug("Response from SQS queue: %s", response)
        return response

    @staticmethod
    def valid_result_message_attributes(sqs_message: MessageTypeDef) -> bool:
        """Validate that "MessageAttributes" field is formatted as expected.

        Args:
            sqs_message: An SQS message to be evaluated.
        """
        valid = False
        if (
            "MessageAttributes" in sqs_message
            and any(
                field
                for field in sqs_message["MessageAttributes"]
                if "PackageID" in field
            )
            and sqs_message["MessageAttributes"]["PackageID"].get("StringValue")
        ):
            valid = True
        else:
            logger.exception("Failed to parse SQS message attributes: %s", sqs_message)
        return valid

    @staticmethod
    def valid_result_message_body(sqs_message: MessageTypeDef) -> bool:
        """Validate that "Body" field is formatted as expected.

        Args:
            sqs_message: An SQS message to be evaluated.
        """
        valid = False
        if "Body" in sqs_message and json.loads(str(sqs_message["Body"])):
            valid = True
        else:
            logger.exception("Failed to parse SQS message body: %s", sqs_message)
        return valid

    def valid_sqs_message(self, sqs_message: MessageTypeDef) -> bool:
        """Validate that an SQS message is formatted as expected.

        Args:
            sqs_message:  An SQS message to be evaluated.

        """
        valid = False
        if (
            self.valid_result_message_attributes(sqs_message=sqs_message)
            and self.valid_result_message_body(sqs_message=sqs_message)
            and sqs_message.get("ReceiptHandle")
        ):
            valid = True
        else:
            logger.exception("Failed to parse SQS message: %s", sqs_message)
        return valid


def filter_log_stream(stream: StringIO) -> str:
    """Filter log stream to only ERROR messages for stakeholder email.

    Args:
        stream: A log stream used to generate an attachment for the stakeholder email.
    """
    stream.seek(0)
    return "".join([line for line in stream if line.startswith("ERROR")])


def get_dois_from_spreadsheet(doi_csv_file: str) -> Iterator[str]:
    """Retriev DOIs from the Wiley-provided CSV file.

    Args:
        doi_csv_file: A CSV file provided by Wiley with DOIs for articles to be processed.
    """
    with smart_open.open(doi_csv_file, encoding="utf-8-sig") as csvfile:
        yield from csvfile.read().splitlines()


def get_crossref_response_from_doi(url: str, doi: str) -> requests.Response:
    """Retrieve Crossref response containing work record based on a DOI.

    Args:
        url: The URL used to request metadata responses.
        doi: The DOI used to request metadata.
    """
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
    """Get response from Wiley server based on a DOI.

    Args:
        url: The URL used to request article content responses.
        doi: The DOI used to request article content.
    """
    logger.debug("Requesting PDF for %s%s", url, doi)
    response = requests.get(f"{url}{doi}", headers=WILEY_HEADERS, timeout=30)
    logger.debug("Response code retrieved from Wiley server for %s: %s", doi, response)
    return response


class InvalidSQSMessageError(Exception):
    pass
