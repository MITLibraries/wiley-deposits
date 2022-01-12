import io
import logging
from datetime import datetime

import click
from botocore.exceptions import ClientError

from awd import config, dynamodb
from awd.dynamodb import DynamoDB
from awd.ses import SES
from awd.sqs import SQS

stream = io.StringIO()
logger = logging.getLogger(__name__)
logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(), logging.StreamHandler(stream)],
)
logging.getLogger("botocore").setLevel(logging.ERROR)


@click.command()
@click.option(
    "--sqs_base_url",
    required=True,
    default=config.SQS_BASE_URL,
    help="The base URL of the SQS queues.",
)
@click.option(
    "--doi_table",
    required=True,
    default=config.DOI_TABLE,
    help="The DynamoDB table containing the state of DOIs in the workflow.",
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
    multiple=True,
    default=config.LOG_RECIPIENT_EMAIL,
    help="The email address receiving the logs. Repeatable",
)
@click.option(
    "--retry_threshold",
    required=True,
    default=config.RETRY_THRESHOLD,
    help="The number of retries to attempt.",
)
def listen(
    sqs_base_url,
    doi_table,
    sqs_output_queue,
    log_source_email,
    log_recipient_email,
    retry_threshold,
):
    date = datetime.today().strftime("%m-%d-%Y %H:%M:%S")
    sqs = SQS()
    dynamodb_client = DynamoDB()
    try:
        for message in sqs.receive(sqs_base_url, sqs_output_queue):
            doi = (
                message.get("MessageAttributes", {})
                .get("PackageID", {})
                .get("StringValue")
            )
            if "'ResultType': 'error'" in message["Body"]:
                logger.error(f'DOI: {doi}, Result: {message.get("Body")}')
                sqs.delete(sqs_base_url, sqs_output_queue, message["ReceiptHandle"])
                if dynamodb_client.retry_attempts_exceeded(
                    doi_table, doi, retry_threshold
                ):
                    dynamodb_client.update_doi_item_status_in_database(
                        doi_table, doi, dynamodb.Status.PERMANENTLY_FAILED
                    )
                else:
                    dynamodb_client.update_doi_item_status_in_database(
                        doi_table, doi, dynamodb.Status.FAILED
                    )
            else:
                logger.info(f'DOI: {doi}, Result: {message.get("Body")}')
                sqs.delete(sqs_base_url, sqs_output_queue, message["ReceiptHandle"])
                dynamodb_client.update_doi_item_status_in_database(
                    doi_table, doi, dynamodb.Status.SUCCESS
                )
        logger.debug("Messages received and deleted from output queue")
    except ClientError as e:
        logger.error(
            f"Failure while retrieving SQS messages, {e.response['Error']['Message']}"
        )
    ses_client = SES()
    message = ses_client.create_email(
        f"DSS results {date}",
        stream.getvalue(),
        f"DSS results {date}.txt",
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


if __name__ == "__main__":
    listen()
