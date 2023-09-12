import datetime
import io
import logging

import click
import sentry_sdk
from botocore.exceptions import ClientError
from pynamodb.exceptions import GetError

from awd.article import (
    Article,
    InvalidArticleContentResponseError,
    InvalidCrossrefMetadataError,
    InvalidDSpaceMetadataError,
    UnprocessedStatusFalseError,
)
from awd.config import AWS_REGION_NAME, DATE_FORMAT, Config
from awd.database import DoiProcessAttempt
from awd.helpers import (
    S3Client,
    SESClient,
    SQSClient,
    get_dois_from_spreadsheet,
)

logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
def cli(
    ctx: click.Context,
) -> None:
    config = Config()
    sentry_dsn = config.SENTRY_DSN
    if sentry_dsn and sentry_dsn.lower() != "none":
        sentry_sdk.init(
            sentry_dsn,
            environment=config.WORKSPACE,
        )
    stream = io.StringIO()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=(getattr(logging, config.LOG_LEVEL) if config.LOG_LEVEL else logging.INFO),
        handlers=[logging.StreamHandler(), logging.StreamHandler(stream)],
    )
    ctx.ensure_object(dict)
    ctx.obj["stream"] = stream
    ctx.obj["config"] = config


@cli.command()
@click.pass_context
def deposit(
    ctx: click.Context,
) -> None:
    """Process a text file of DOIs to retrieve metadata and PDFs and send to SQS queue.

    Errors generated during the process are emailed to stakeholders.
    """
    date = datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT)
    stream = ctx.obj["stream"]
    config = ctx.obj["config"]
    s3_client = S3Client()
    sqs_client = SQSClient(
        region=AWS_REGION_NAME,
        base_url=config.SQS_BASE_URL,
        queue_name=config.SQS_INPUT_QUEUE,
    )
    try:
        s3_client.client.list_objects_v2(Bucket=config.BUCKET)
    except ClientError as e:
        logger.exception(
            "Error accessing bucket: %s, %s",
            config.BUCKET,
            e.response["Error"]["Message"],
        )
        return  # Unable to access S3 bucket, exit application
    DoiProcessAttempt.set_table_name(config.DOI_TABLE)
    if not DoiProcessAttempt.exists():
        logger.exception("Unable to read DynamoDB table")
        return  # exit application

    for doi_file in s3_client.retrieve_file_type_from_bucket(
        config.BUCKET, ".csv", "archived"
    ):
        dois = get_dois_from_spreadsheet(f"s3://{config.BUCKET}/{doi_file}")
        for doi in dois:
            logger.error(doi)
            article = Article(
                doi=doi,
                metadata_url=config.METADATA_URL,
                content_url=config.CONTENT_URL,
                s3_client=s3_client,
                bucket=config.BUCKET,
                sqs_client=sqs_client,
                sqs_base_url=config.SQS_BASE_URL,
                sqs_input_queue=config.SQS_INPUT_QUEUE,
                sqs_output_queue=config.SQS_OUTPUT_QUEUE,
                collection_handle=config.COLLECTION_HANDLE,
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
            bucket=config.BUCKET, key=doi_file, archived_key_prefix="archived"
        )
    logger.debug("Submission process has completed")

    # Send logs as email via SES
    ses_client = SESClient(AWS_REGION_NAME)
    ses_client.create_and_send_email(
        subject=f"Automated Wiley deposit errors {date}",
        attachment_content=stream.getvalue(),
        attachment_name=f"{date}_submission_log.txt",
        source_email_address=config.LOG_SOURCE_EMAIL,
        recipient_email_address=config.LOG_RECIPIENT_EMAIL,
    )


@cli.command()
@click.pass_context
def listen(
    ctx: click.Context,
) -> None:
    """Retrieve messages from an SQS queue and email the results to stakeholders."""
    date = datetime.datetime.now(tz=datetime.UTC).strftime(DATE_FORMAT)
    stream = ctx.obj["stream"]
    config = ctx.obj["config"]
    sqs_client = SQSClient(
        region=AWS_REGION_NAME,
        base_url=config.SQS_BASE_URL,
        queue_name=config.SQS_OUTPUT_QUEUE,
    )

    DoiProcessAttempt.set_table_name(config.DOI_TABLE)

    for sqs_message in sqs_client.receive():
        try:
            sqs_client.process_result_message(
                sqs_message=sqs_message,
                retry_threshold=config.RETRY_THRESHOLD,
            )
        except:  # noqa: E722
            logger.exception("Error while processing SQS message: %s", sqs_message)
            continue
    logger.debug("Messages received and deleted from output queue")

    ses_client = SESClient(AWS_REGION_NAME)
    ses_client.create_and_send_email(
        subject=f"DSS results {date}",
        attachment_content=stream.getvalue(),
        attachment_name=f"DSS results {date}.txt",
        source_email_address=config.LOG_SOURCE_EMAIL,
        recipient_email_address=config.LOG_RECIPIENT_EMAIL,
    )
