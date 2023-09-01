import datetime
import io
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
    UnprocessedStatusFalseError,
)
from awd.database import DoiProcessAttempt
from awd.helpers import (
    InvalidSQSMessageError,
    S3Client,
    SESClient,
    SQSClient,
    get_dois_from_spreadsheet,
)

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
    "--doi_table_name",
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
    doi_table_name: str,
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
    ctx.obj["doi_table_name"] = doi_table_name
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
    s3_client = S3Client()
    sqs_client = SQSClient(
        region=ctx.obj["aws_region"],
        base_url=ctx.obj["sqs_base_url"],
        queue_name=sqs_input_queue,
    )

    try:
        s3_client.client.list_objects_v2(Bucket=bucket)
    except ClientError as e:
        logger.exception(
            "Error accessing bucket: %s, %s", bucket, e.response["Error"]["Message"]
        )
        return  # Unable to access S3 bucket, exit application

    DoiProcessAttempt.set_table_name(ctx.obj["doi_table_name"])
    if not DoiProcessAttempt.exists():
        logger.exception("Unable to read DynamoDB table")
        return  # exit application

    for doi_file in s3_client.retrieve_file_type_from_bucket(bucket, ".csv", "archived"):
        dois = get_dois_from_spreadsheet(f"s3://{bucket}/{doi_file}")

        for doi in dois:
            article = Article(
                doi=doi,
                metadata_url=metadata_url,
                content_url=content_url,
                s3_client=s3_client,
                bucket=bucket,
                sqs_client=sqs_client,
                sqs_base_url=ctx.obj["sqs_base_url"],
                sqs_input_queue=sqs_input_queue,
                sqs_output_queue=ctx.obj["sqs_output_queue"],
                collection_handle=collection_handle,
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

        s3_client.archive_file_with_new_key(
            bucket=bucket, key=doi_file, archived_key_prefix="archived"
        )
    logger.debug("Submission process has completed")

    # Send logs as email via SES
    ses_client = SESClient(ctx.obj["aws_region"])

    try:
        ses_client.create_and_send_email(
            subject=f"Automated Wiley deposit errors {date}",
            attachment_content=stream.getvalue(),
            attachment_name=f"{date}_submission_log.txt",
            source_email_address=ctx.obj["log_source_email"],
            recipient_email_address=ctx.obj["log_recipient_email"],
        )
    except ClientError as e:
        logger.exception(
            "Failed to send deposit logs: %s", e.response["Error"]["Message"]
        )


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
    sqs_client = SQSClient(
        region=ctx.obj["aws_region"],
        base_url=ctx.obj["sqs_base_url"],
        queue_name=ctx.obj["sqs_output_queue"],
    )

    DoiProcessAttempt.set_table_name(ctx.obj["doi_table_name"])

    try:
        for sqs_message in sqs_client.receive():
            try:
                sqs_client.process_result_message(
                    sqs_message=sqs_message,
                    retry_threshold=retry_threshold,
                )
            except (KeyError, ClientError, InvalidSQSMessageError):
                logger.exception("Error while processing SQS message: %s", sqs_message)
                continue
        logger.debug("Messages received and deleted from output queue")
    except ClientError as e:
        logger.exception(
            "Error while retrieving messages from SQS queue: %s",
            e.response["Error"]["Message"],
        )

    ses_client = SESClient(ctx.obj["aws_region"])

    try:
        ses_client.create_and_send_email(
            subject=f"DSS results {date}",
            attachment_content=stream.getvalue(),
            attachment_name=f"DSS results {date}.txt",
            source_email_address=ctx.obj["log_source_email"],
            recipient_email_address=ctx.obj["log_recipient_email"],
        )
    except ClientError as e:
        logger.exception("Failed to send listen logs: %s", e.response["Error"]["Message"])
