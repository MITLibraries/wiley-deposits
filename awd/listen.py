import io
import logging
from datetime import datetime

import click
from botocore.exceptions import ClientError

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
    help="The base URL of the SQS queues.",
)
@click.option(
    "--sqs_output_queue",
    required=True,
    help="The SQS queue from which results messages will be received.",
)
@click.option(
    "--log_source_email",
    required=True,
    help="The email address sending the logs.",
)
@click.option(
    "--log_recipient_email",
    required=True,
    multiple=True,
    help="The email address receiving the logs. Repeatable",
)
def listen(sqs_base_url, sqs_output_queue, log_source_email, log_recipient_email):
    date = datetime.today().strftime("%m-%d-%Y %H:%M:%S")
    sqs = SQS()
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
            else:
                logger.info(f'DOI: {doi}, Result: {message.get("Body")}')
                sqs.delete(sqs_base_url, sqs_output_queue, message["ReceiptHandle"])
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
