import datetime
import io
import json
import logging

import click
import sentry_sdk
from botocore.exceptions import ClientError
from pynamodb.exceptions import GetError

from awd import config
from awd.article import (
    Article,
    InvalidArticleContentResponseError,
    InvalidCrossrefMetadataError,
    InvalidDSpaceMetadataError,
)
from awd.database import DoiProcessAttempt, UnprocessedStatusFalseError
from awd.helpers import S3, SES, SQS, get_dois_from_spreadsheet
from awd.status import Status

logger = logging.getLogger(__name__)


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
    s3_client = S3()
    sqs_client = SQS(ctx.obj["aws_region"])

    try:
        s3_client.client.list_objects_v2(Bucket=bucket)
    except ClientError as e:
        logger.exception(
            "Error accessing bucket: %s, %s", bucket, e.response["Error"]["Message"]
        )
        return  # Unable to access S3 bucket, exit application

    doi_table = DoiProcessAttempt()
    doi_table.set_table_name(ctx.obj["doi_table"])
    if not doi_table.exists():
        logger.exception("Unable to read DynamoDB table")
        return  # exit application

    for doi_file in s3_client.filter_files_in_bucket(bucket, ".csv", "archived"):
        dois = get_dois_from_spreadsheet(f"s3://{bucket}/{doi_file}")

        for doi in dois:
            article = Article(
                doi,
                metadata_url,
                content_url,
                doi_table,
                s3_client,
                bucket,
                sqs_client,
                ctx.obj["sqs_base_url"],
                sqs_input_queue,
                ctx.obj["sqs_output_queue"],
                collection_handle,
            )
            try:
                article.process()
            except (
                ClientError,
                GetError,
                InvalidArticleContentResponseError,
                InvalidCrossrefMetadataError,
                InvalidDSpaceMetadataError,
                UnprocessedStatusFalseError,
            ):
                logger.exception(article.doi)
                continue

        s3_client.archive_file_with_new_key(bucket, doi_file, "archived")
    logger.debug("Submission process has completed")

    # Send logs as email via SES
    ses_client = SES(ctx.obj["aws_region"])

    try:
        ses_client.create_and_send_email(
            subject=f"Automated Wiley deposit errors {date}",
            attachment_content=stream.getvalue(),
            attachment_name=f"{date}_submission_log.txt",
            source_email_address=ctx.obj["log_source_email"],
            recipient_email_address=ctx.obj["log_recipient_email"],
        )
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
    retry_threshold: str,
) -> None:
    """Retrieve messages from an SQS queue and email the results to stakeholders."""
    date = datetime.datetime.now(tz=datetime.UTC).strftime(config.DATE_FORMAT)
    stream = ctx.obj["stream"]
    sqs = SQS(ctx.obj["aws_region"])

    doi_table = DoiProcessAttempt()
    doi_table.set_table_name(ctx.obj["doi_table"])

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
                    if doi_table.attempts_exceeded(doi, int(retry_threshold)):
                        doi_table.update_status(doi, Status.FAILED.value)
                    else:
                        doi_table.update_status(doi, Status.UNPROCESSED.value)
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
                    doi_table.update_status(doi, Status.SUCCESS.value)
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
