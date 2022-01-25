import io
import json
import logging
from datetime import datetime

import click
from botocore.exceptions import ClientError

from awd import config, crossref, dynamodb, s3, ses, sqs, wiley
from awd.dynamodb import DynamoDB
from awd.s3 import S3
from awd.ses import SES
from awd.sqs import SQS
from awd.ssm import SSM
from awd.status import Status

logger = logging.getLogger(__name__)


def doi_to_be_added(doi, doi_items):
    "Validate that a DOI is not a part of the database table and needs to  be added."
    validation_status = False
    if not any(doi_item["doi"] == doi for doi_item in doi_items):
        validation_status = True
        logger.debug(f"{doi} added to database.")
    return validation_status


def doi_to_be_retried(doi, doi_items):
    "Validate that a DOI should be retried based on its status in the database table."
    validation_status = False
    for doi_item in [
        d
        for d in doi_items
        if d["doi"] == doi and d["status"] == str(Status.FAILED.value)
    ]:
        validation_status = True
        logger.debug(f"{doi} will be retried.")
    return validation_status


@click.group()
@click.option(
    "--aws_region",
    required=True,
    default=config.AWS_REGION_NAME,
    help="The AWS region to use for clients.",
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
    ctx,
    aws_region,
    doi_table,
    sqs_base_url,
    sqs_output_queue,
    log_source_email,
    log_recipient_email,
):
    stream = io.StringIO()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(message)s",
        level=logging.INFO,
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
    ctx,
    metadata_url,
    content_url,
    bucket,
    sqs_input_queue,
    collection_handle,
):
    date = datetime.today().strftime("%m-%d-%Y %H:%M:%S")
    stream = ctx.obj["stream"]
    s3_client = s3.S3()
    sqs_client = sqs.SQS(ctx.obj["aws_region"])
    dynamodb_client = dynamodb.DynamoDB(ctx.obj["aws_region"])

    try:
        s3_client.client.list_objects_v2(Bucket=bucket)
    except ClientError as e:
        logger.error(
            f"Error accessing bucket: {bucket}, {e.response['Error']['Message']}"
        )
        exit
    for doi_file in s3_client.filter_files_in_bucket(bucket, ".csv", "archived"):
        dois = crossref.get_dois_from_spreadsheet(f"s3://{bucket}/{doi_file}")
        doi_items = dynamodb_client.retrieve_doi_items_from_database(
            ctx.obj["doi_table"]
        )
        for doi in dois:
            if doi_to_be_added(doi, doi_items):
                dynamodb_client.add_doi_item_to_database(ctx.obj["doi_table"], doi)
            elif doi_to_be_retried(doi, doi_items) is False:
                continue
            dynamodb_client.update_doi_item_status_in_database(
                ctx.obj["doi_table"], doi, Status.PROCESSING.value
            )
            dynamodb_client.update_doi_item_attempts_in_database(
                ctx.obj["doi_table"], doi
            )
            crossref_work_record = crossref.get_work_record_from_doi(metadata_url, doi)
            if crossref.is_valid_response(doi, crossref_work_record) is False:
                dynamodb_client.update_doi_item_status_in_database(
                    ctx.obj["doi_table"], doi, Status.FAILED.value
                )
                continue
            value_dict = crossref.get_metadata_extract_from(crossref_work_record)
            metadata = crossref.create_dspace_metadata_from_dict(
                value_dict, "config/metadata_mapping.json"
            )
            if crossref.is_valid_dspace_metadata(metadata) is False:
                dynamodb_client.update_doi_item_status_in_database(
                    ctx.obj["doi_table"], doi, Status.FAILED.value
                )
                continue
            wiley_response = wiley.get_wiley_response(content_url, doi)
            if wiley.is_valid_response(doi, wiley_response) is False:
                dynamodb_client.update_doi_item_status_in_database(
                    ctx.obj["doi_table"], doi, Status.FAILED.value
                )
                continue
            doi_file_name = doi.replace(
                "/", "-"
            )  # 10.1002/term.3131 to 10.1002-term.3131
            files_dict = s3.create_files_dict(
                doi_file_name, json.dumps(metadata), wiley_response.content
            )
            try:
                for file in files_dict:
                    s3_client.put_file(file["file_content"], bucket, file["file_name"])
            except ClientError as e:
                logger.error(
                    f"Upload failed: {file['file_name']},"
                    f"{e.response['Error']['Message']}"
                )
                dynamodb_client.update_doi_item_status_in_database(
                    ctx.obj["doi_table"], doi, Status.FAILED.value
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
        s3_client.archive_file_with_new_key(bucket, doi_file, "archived")
    logger.debug("Submission process has completed")

    ses_client = ses.SES(ctx.obj["aws_region"])
    message = ses_client.create_email(
        f"Automated Wiley deposit errors {date}",
        stream.getvalue(),
        f"{date}_submission_log.txt",
    )
    try:
        ses_client.send_email(
            ctx.obj["log_source_email"],
            ctx.obj["log_recipient_email"],
            message,
        )
        logger.debug(f"Logs sent to {ctx.obj['log_recipient_email']}")
    except ClientError as e:
        logger.error(f"Failed to send logs: {e.response['Error']['Message']}")


@cli.command()
@click.option(
    "--retry_threshold",
    required=True,
    default=config.RETRY_THRESHOLD,
    help="The number of retries to attempt.",
)
@click.pass_context
def listen(
    ctx,
    retry_threshold,
):
    date = datetime.today().strftime("%m-%d-%Y %H:%M:%S")
    stream = ctx.obj["stream"]
    sqs = SQS(ctx.obj["aws_region"])
    dynamodb_client = DynamoDB(ctx.obj["aws_region"])
    try:
        for message in sqs.receive(
            ctx.obj["sqs_base_url"], ctx.obj["sqs_output_queue"]
        ):
            doi = (
                message.get("MessageAttributes", {})
                .get("PackageID", {})
                .get("StringValue")
            )
            if "'ResultType': 'error'" in message["Body"]:
                logger.error(f'DOI: {doi}, Result: {message.get("Body")}')
                sqs.delete(
                    ctx.obj["sqs_base_url"],
                    ctx.obj["sqs_output_queue"],
                    message["ReceiptHandle"],
                )
                if dynamodb_client.retry_attempts_exceeded(
                    ctx.obj["doi_table"], doi, retry_threshold
                ):
                    dynamodb_client.update_doi_item_status_in_database(
                        ctx.obj["doi_table"], doi, Status.PERMANENTLY_FAILED.value
                    )
                else:
                    dynamodb_client.update_doi_item_status_in_database(
                        ctx.obj["doi_table"], doi, Status.FAILED.value
                    )
            else:
                logger.info(f'DOI: {doi}, Result: {message.get("Body")}')
                sqs.delete(
                    ctx.obj["sqs_base_url"],
                    ctx.obj["sqs_output_queue"],
                    message["ReceiptHandle"],
                )

                dynamodb_client.update_doi_item_status_in_database(
                    ctx.obj["doi_table"], doi, Status.SUCCESS.value
                )
        logger.debug("Messages received and deleted from output queue")
    except ClientError as e:
        logger.error(
            f"Failure while retrieving SQS messages, {e.response['Error']['Message']}"
        )
    ses_client = SES(ctx.obj["aws_region"])
    message = ses_client.create_email(
        f"DSS results {date}",
        stream.getvalue(),
        f"DSS results {date}.txt",
    )
    try:
        ses_client.send_email(
            ctx.obj["log_source_email"],
            ctx.obj["log_recipient_email"],
            message,
        )
        logger.debug(f"Logs sent to {ctx.obj['log_recipient_email']}")
    except ClientError as e:
        logger.error(f"Failed to send logs: {e.response['Error']['Message']}")


@cli.command()
def check_permissions():
    """Confirm AWD infrastructure has deployed properly with correct permissions to all
    expected resources given the current env configuration.

    Note: Only useful in stage and prod envs, as this command requires SSM access which
        does not get configured in dev.

    Note: Checking SQS write permissions does write a message to each configured output
        queue. The test message gets deleted as part of the process, however if there
        are already more than 10 messages in the output queue that delete may not
        happen and the test message will remain in the queue. It is best to only run
        this command when the configured output queues are empty.
    """
    dynamodb = DynamoDB()
    logger.info(dynamodb.check_read_permissions(config.DOI_TABLE))
    logger.info(dynamodb.check_write_permissions(config.DOI_TABLE))

    s3 = S3()
    logger.info(s3.check_permissions(config.BUCKET))

    ses = SES()
    logger.info(ses.check_permissions(config.LOG_SOURCE_EMAIL, "mock@mock.mock"))

    ssm = SSM()
    for path in [config.DSS_SSM_PATH, config.WILEY_SSM_PATH]:
        logger.info(ssm.check_permissions(path))

    sqs = SQS()
    logger.info(
        sqs.check_write_permissions(config.SQS_BASE_URL, config.SQS_INPUT_QUEUE)
    )
    logger.info(
        sqs.check_read_permissions(config.SQS_BASE_URL, config.SQS_OUTPUT_QUEUE)
    )

    logger.info(f"All permissions confirmed for env: {config.ENV}")
