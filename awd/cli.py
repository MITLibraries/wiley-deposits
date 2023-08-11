from __future__ import annotations

import datetime
import io
import json
import logging
import sys
from typing import Any

import click
import sentry_sdk
from botocore.exceptions import ClientError

from awd import config, crossref, dynamodb, s3, ses, sqs, wiley
from awd.dynamodb import DynamoDB
from awd.ses import SES
from awd.sqs import SQS
from awd.status import Status

logger = logging.getLogger(__name__)


def create_list_of_dspace_item_files(
    file_name: str, metadata_content: str, bitstream_content: bytes
) -> list[tuple[str, str | bytes]]:
    """Create a list of metadata and content tuples for a DSpace item."""
    return [
        (
            f"{file_name}.json",
            metadata_content,
        ),
        (
            f"{file_name}.pdf",
            bitstream_content,
        ),
    ]


def doi_to_be_added(doi: str, doi_items: list[dict[str, Any]]) -> bool:
    """Validate that a DOI is not a part of the database table and needs to  be added."""
    validation_status = False
    if not any(doi_item["doi"] == doi for doi_item in doi_items):
        validation_status = True
        logger.debug("%s added to database", doi)
    return validation_status


def doi_to_be_retried(doi: str, doi_items: list[dict[str, Any]]) -> bool:
    """Validate that a DOI should be retried based on its status in the database table."""
    validation_status = False
    for _doi_item in [
        d
        for d in doi_items
        if d["doi"] == doi and d["status"] == str(Status.UNPROCESSED.value)
    ]:
        validation_status = True
        logger.debug("%s will be retried", doi)
    return validation_status


@click.group()
@click.option(
    "--aws_region",
    required=True,
    default=config.AWS_REGION_NAME,
    help="The AWS region to use for clients.",
)
@click.option(
    "--log_level",
    required=True,
    default=config.LOG_LEVEL,
    help="The log level to use.",
)
@click.option(
    "--doi_table",
    required=True,
    default=config.DOI_TABLE,
    help="The DynamoDB table containing the state of DOIs in the workflow.",
)
@click.option(
    "--sqs_base_url",
    required=True,
    default=config.SQS_BASE_URL,
    help="The base URL of the SQS queues.",
)
@click.option(
    "--sqs_output_queue",
    required=True,
    default=config.SQS_OUTPUT_QUEUE,
    help="The SQS queue from which results messages will be received.",
)
@click.option(
    "--log_source_email",
    required=True,
    default=config.LOG_SOURCE_EMAIL,
    help="The email address sending the logs.",
)
@click.option(
    "--log_recipient_email",
    required=True,
    default=config.LOG_RECIPIENT_EMAIL,
    help="The email address receiving the logs.",
)
@click.pass_context
def cli(
    ctx: click.Context,
    aws_region: str,
    log_level: str,
    doi_table: str,
    sqs_base_url: str,
    sqs_output_queue: str,
    log_source_email: str,
    log_recipient_email: str,
) -> None:
    sentry_dsn = config.SENTRY_DSN
    if sentry_dsn and sentry_dsn.lower() != "none":
        sentry_sdk.init(sentry_dsn, environment=config.ENV)
    stream = io.StringIO()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=getattr(logging, log_level) if log_level else logging.INFO,
        handlers=[logging.StreamHandler(), logging.StreamHandler(stream)],
    )
    ctx.ensure_object(dict)
    ctx.obj["stream"] = stream
    ctx.obj["aws_region"] = aws_region
    ctx.obj["doi_table"] = doi_table
    ctx.obj["sqs_base_url"] = sqs_base_url
    ctx.obj["sqs_output_queue"] = sqs_output_queue
    ctx.obj["log_source_email"] = log_source_email
    ctx.obj["log_recipient_email"] = log_recipient_email


@cli.command()
@click.option(
    "--metadata_url",
    required=True,
    default=config.METADATA_URL,
    help="The URL of the metadata records.",
)
@click.option(
    "--content_url",
    required=True,
    default=config.CONTENT_URL,
    help="The URL of the content files.",
)
@click.option(
    "--bucket",
    required=True,
    default=config.BUCKET,
    help="The bucket to which content and metadata files will be uploaded.",
)
@click.option(
    "--sqs_input_queue",
    required=True,
    default=config.SQS_INPUT_QUEUE,
    help="The SQS queue to which submission messages will be sent.",
)
@click.option(
    "--collection_handle",
    required=True,
    default=config.COLLECTION_HANDLE,
    help="The handle of the DSpace collection to which items will be added.",
)
@click.pass_context
def deposit(
    ctx: click.Context,
    metadata_url: str,
    content_url: str,
    bucket: str,
    sqs_input_queue: str,
    collection_handle: str,
) -> None:
    """Process a text file of DOIs to retrieve metadata and PDFs and send to SQS queue.

    Errors generated during the process are emailed to stakeholders.
    """
    date = datetime.datetime.now(tz=datetime.UTC).strftime(config.DATE_FORMAT)
    stream = ctx.obj["stream"]
    s3_client = s3.S3()
    sqs_client = sqs.SQS(ctx.obj["aws_region"])
    dynamodb_client = dynamodb.DynamoDB(ctx.obj["aws_region"])

    try:
        s3_client.client.list_objects_v2(Bucket=bucket)
    except ClientError as e:
        logger.exception(
            "Error accessing bucket: %s, %s", bucket, e.response["Error"]["Message"]
        )
        sys.exit()
    for doi_file in s3_client.filter_files_in_bucket(bucket, ".csv", "archived"):
        dois = crossref.get_dois_from_spreadsheet(f"s3://{bucket}/{doi_file}")
        try:
            doi_items = dynamodb_client.retrieve_doi_items_from_database(
                ctx.obj["doi_table"]
            )
        except ClientError as e:
            logger.exception("Table read failed: %s", e.response["Error"]["Message"])
            sys.exit()
        for doi in dois:
            if doi_to_be_added(doi, doi_items):
                try:
                    dynamodb_client.add_doi_item_to_database(ctx.obj["doi_table"], doi)
                except ClientError as e:
                    logger.exception(
                        "Table error while processing %s: %s",
                        doi,
                        e.response["Error"]["Message"],
                    )
            elif doi_to_be_retried(doi, doi_items) is False:
                continue
            try:
                dynamodb_client.update_doi_item_attempts_in_database(
                    ctx.obj["doi_table"], doi
                )
            except KeyError:
                logger.exception("Key error in table while processing %s", doi)
            except ClientError as e:
                logger.exception(
                    "Table error while processing %s: %s",
                    doi,
                    e.response["Error"]["Message"],
                )
            crossref_work_record = crossref.get_work_record_from_doi(metadata_url, doi)
            if crossref.is_valid_response(doi, crossref_work_record) is False:
                continue
            value_dict = crossref.get_metadata_extract_from(crossref_work_record)
            metadata = crossref.create_dspace_metadata_from_dict(
                value_dict, "config/metadata_mapping.json"
            )
            if crossref.is_valid_dspace_metadata(metadata) is False:
                continue
            wiley_response = wiley.get_wiley_response(content_url, doi)
            if wiley.is_valid_response(doi, wiley_response) is False:
                continue
            doi_file_name = doi.replace(
                "/", "-"
            )  # 10.1002/term.3131 to 10.1002-term.3131
            try:
                for file_name, file_contents in create_list_of_dspace_item_files(
                    doi_file_name, json.dumps(metadata), wiley_response.content
                ):
                    s3_client.put_file(file_contents, bucket, file_name)
            except ClientError as e:
                logger.exception(
                    "Upload failed for  %s: %s", file_name, e.response["Error"]["Message"]
                )
                continue
            bitstream_s3_uri = f"s3://{bucket}/{doi_file_name}.pdf"
            metadata_s3_uri = f"s3://{bucket}/{doi_file_name}.json"
            dss_message_attributes = sqs.create_dss_message_attributes(
                doi, "wiley", ctx.obj["sqs_output_queue"]
            )
            dss_message_body = sqs.create_dss_message_body(
                "DSpace@MIT",
                collection_handle,
                metadata_s3_uri,
                f"{doi_file_name}.pdf",
                bitstream_s3_uri,
            )
            sqs_client.send(
                ctx.obj["sqs_base_url"],
                sqs_input_queue,
                dss_message_attributes,
                dss_message_body,
            )
            try:
                dynamodb_client.update_doi_item_status_in_database(
                    ctx.obj["doi_table"], doi, Status.MESSAGE_SENT.value
                )
            except KeyError:
                logger.exception("Key error in table while processing %s", doi)
            except ClientError as e:
                logger.exception(
                    "Table error while processing %s: %s",
                    doi,
                    e.response["Error"]["Message"],
                )
        s3_client.archive_file_with_new_key(bucket, doi_file, "archived")
    logger.debug("Submission process has completed")

    ses_client = ses.SES(ctx.obj["aws_region"])
    email_message = ses_client.create_email(
        f"Automated Wiley deposit errors {date}",
        stream.getvalue(),
        f"{date}_submission_log.txt",
    )
    try:
        ses_client.send_email(
            ctx.obj["log_source_email"],
            ctx.obj["log_recipient_email"],
            email_message,
        )
        logger.debug("Logs sent to %s", ctx.obj["log_recipient_email"])
    except ClientError as e:
        logger.exception("Failed to send logs: %s", e.response["Error"]["Message"])


@cli.command()
@click.option(
    "--retry_threshold",
    required=True,
    default=config.RETRY_THRESHOLD,
    help="The number of retries to attempt.",
)
@click.pass_context
def listen(
    ctx: click.Context,
    retry_threshold: int,
) -> None:
    """Retrieve messages from an SQS queue and email the results to stakeholders."""
    date = datetime.datetime.now(tz=datetime.UTC).strftime(config.DATE_FORMAT)
    stream = ctx.obj["stream"]
    sqs = SQS(ctx.obj["aws_region"])
    dynamodb_client = DynamoDB(ctx.obj["aws_region"])
    try:
        for sqs_message in sqs.receive(
            ctx.obj["sqs_base_url"], ctx.obj["sqs_output_queue"]
        ):
            try:
                doi = sqs_message["MessageAttributes"]["PackageID"]["StringValue"]
            except KeyError:
                logger.exception(
                    "Failed to get DOI from message attributes: %s", sqs_message
                )
                continue
            try:
                body = json.loads(str(sqs_message.get("Body")))
            except ValueError:
                logger.exception("Failed to parse body of SQS message: %s", sqs_message)
                continue
            if body["ResultType"] == "error":
                logger.exception("DOI: %s, Result: %s", doi, body)
                sqs.delete(
                    ctx.obj["sqs_base_url"],
                    ctx.obj["sqs_output_queue"],
                    sqs_message["ReceiptHandle"],
                )
                try:
                    if dynamodb_client.attempts_exceeded(
                        ctx.obj["doi_table"], doi, retry_threshold
                    ):
                        dynamodb_client.update_doi_item_status_in_database(
                            ctx.obj["doi_table"], doi, Status.FAILED.value
                        )
                    else:
                        dynamodb_client.update_doi_item_status_in_database(
                            ctx.obj["doi_table"], doi, Status.UNPROCESSED.value
                        )
                except KeyError:
                    logger.exception(
                        "Key error in table while processing %s",
                        doi,
                    )
                except ClientError as e:
                    logger.exception(
                        "Table error while processing %s: %s",
                        doi,
                        e.response["Error"]["Message"],
                    )
            else:
                logger.info("DOI: %s, Result: %s", doi, body)
                sqs.delete(
                    ctx.obj["sqs_base_url"],
                    ctx.obj["sqs_output_queue"],
                    sqs_message["ReceiptHandle"],
                )
                try:
                    dynamodb_client.update_doi_item_status_in_database(
                        ctx.obj["doi_table"], doi, Status.SUCCESS.value
                    )
                except KeyError:
                    logger.exception(
                        "Key error in table while processing %s",
                        doi,
                    )
                except ClientError as e:
                    logger.exception(
                        "Table error while processing %s: %s",
                        doi,
                        e.response["Error"]["Message"],
                    )
        logger.debug("Messages received and deleted from output queue")
    except ClientError as e:
        logger.exception(
            "Failure while retrieving SQS messages: %s", e.response["Error"]["Message"]
        )
    ses_client = SES(ctx.obj["aws_region"])
    email_message = ses_client.create_email(
        f"DSS results {date}",
        stream.getvalue(),
        f"DSS results {date}.txt",
    )
    try:
        ses_client.send_email(
            ctx.obj["log_source_email"],
            ctx.obj["log_recipient_email"],
            email_message,
        )
        logger.debug("Logs sent to %s", ctx.obj["log_recipient_email"])
    except ClientError as e:
        logger.exception("Failed to send logs: %s", e.response["Error"]["Message"])
