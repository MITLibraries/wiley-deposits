import io
import json
import logging
from datetime import datetime
from enum import Enum

import click
from botocore.exceptions import ClientError

from awd import config, crossref, dynamodb, s3, ses, sqs, wiley

stream = io.StringIO()
logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.ERROR,
    handlers=[logging.StreamHandler(), logging.StreamHandler(stream)],
)


class Status(Enum):
    PROCESSING = 1
    SUCCESS = 2
    FAILED = 3
    PERMANENTLY_FAILED = 4


@click.command()
@click.option(
    "--doi_file_path",
    required=True,
    default=config.DOI_FILE_PATH,
    help="The path of the spreadsheet containing DOIs to be searched.",
)
@click.option(
    "--doi_table",
    required=True,
    default=config.DOI_TABLE,
    help="The DynamoDB table containing the state of DOIs in the workflow.",
)
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
    "--sqs_base_url",
    required=True,
    default=config.SQS_BASE_URL,
    help="The base URL of the SQS queues.",
)
@click.option(
    "--sqs_input_queue",
    required=True,
    default=config.SQS_INPUT_QUEUE,
    help="The SQS queue to which submission messages will be sent.",
)
@click.option(
    "--sqs_output_queue",
    required=True,
    default=config.SQS_OUTPUT_QUEUE,
    help="The SQS queue from which results messages will be received.",
)
@click.option(
    "--collection_handle",
    required=True,
    default=config.COLLECTION_HANDLE,
    help="The handle of the DSpace collection to which items will be added.",
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
    multiple=True,
    default=config.LOG_RECIPIENT_EMAIL,
    help="The email address receiving the logs. Repeatable",
)
def deposit(
    doi_file_path,
    doi_table,
    metadata_url,
    content_url,
    bucket,
    sqs_base_url,
    sqs_input_queue,
    sqs_output_queue,
    collection_handle,
    log_source_email,
    log_recipient_email,
):
    date = datetime.today().strftime("%m-%d-%Y %H:%M:%S")
    s3_client = s3.S3()
    sqs_client = sqs.SQS()
    dynamodb_client = dynamodb.DynamoDB()
    dois = crossref.get_dois_from_spreadsheet(doi_file_path)
    doi_items = dynamodb_client.retrieve_doi_items_from_database(doi_table)
    for doi in dois:
        if doi_to_be_added(doi, doi_items):
            dynamodb_client.add_doi_item_to_database(doi_table, doi)
        elif doi_to_be_retried(doi, doi_items) is False:
            continue
        dynamodb_client.update_doi_item_status_in_database(
            doi_table, doi, Status.PROCESSING.value
        )
        dynamodb_client.update_doi_item_attempts_in_database(doi_table, doi)
        crossref_work_record = crossref.get_work_record_from_doi(metadata_url, doi)
        if crossref.is_valid_response(doi, crossref_work_record) is False:
            dynamodb_client.update_doi_item_status_in_database(
                doi_table, doi, Status.FAILED.value
            )
            continue
        value_dict = crossref.get_metadata_extract_from(crossref_work_record)
        metadata = crossref.create_dspace_metadata_from_dict(
            value_dict, "config/metadata_mapping.json"
        )
        if crossref.is_valid_dspace_metadata(metadata) is False:
            dynamodb_client.update_doi_item_status_in_database(
                doi_table, doi, Status.FAILED.value
            )
            continue
        wiley_response = wiley.get_wiley_response(content_url, doi)
        if wiley.is_valid_response(doi, wiley_response) is False:
            dynamodb_client.update_doi_item_status_in_database(
                doi_table, doi, Status.FAILED.value
            )
            continue
        doi_file_name = doi.replace("/", "-")  # 10.1002/term.3131 to 10.1002-term.3131
        files_dict = s3.create_files_dict(
            doi_file_name, json.dumps(metadata), wiley_response.content
        )
        try:
            for file in files_dict:
                s3_client.put_file(file["file_content"], bucket, file["file_name"])
        except ClientError as e:
            logger.error(
                f"Upload failed: {file['file_name']}, {e.response['Error']['Message']}"
            )
            dynamodb_client.update_doi_item_status_in_database(
                doi_table, doi, Status.FAILED.value
            )
            continue
        bitstream_s3_uri = f"s3://{bucket}/{doi_file_name}.pdf"
        metadata_s3_uri = f"s3://{bucket}/{doi_file_name}.json"
        dss_message_attributes = sqs.create_dss_message_attributes(
            doi_file_name, "wiley", sqs_output_queue
        )
        dss_message_body = sqs.create_dss_message_body(
            "DSpace@MIT",
            collection_handle,
            metadata_s3_uri,
            f"{doi_file_name}.pdf",
            bitstream_s3_uri,
        )
        sqs_client.send(
            sqs_base_url,
            sqs_input_queue,
            dss_message_attributes,
            dss_message_body,
        )
    logger.info("Submission process has completed")

    ses_client = ses.SES()
    message = ses_client.create_email(
        f"Automated Wiley deposit errors {date}",
        stream.getvalue(),
        f"{date}_submission_log.txt",
    )
    try:
        ses_client.send_email(
            log_source_email,
            list(log_recipient_email),
            message,
        )
        logger.info(f"Logs sent to {list(log_recipient_email)}")
    except ClientError as e:
        logger.error(f"Failed to send logs: {e.response['Error']['Message']}")


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
        d for d in doi_items if d["doi"] == doi and d["status"] == "Failed, will retry"
    ]:
        validation_status = True
        logger.debug(f"{doi} will be retried.")
    return validation_status


if __name__ == "__main__":
    deposit()
