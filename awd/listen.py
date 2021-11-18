import logging

import click
from botocore.exceptions import ClientError

from awd.sqs import SQS

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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
def listen(sqs_base_url, sqs_output_queue):
    sqs = SQS()
    try:
        for message in sqs.receive(sqs_base_url, sqs_output_queue):
            if "'ResultType': 'error'" in message["Body"]:
                logger.error(message["Body"])
                sqs.delete(sqs_base_url, sqs_output_queue, message["ReceiptHandle"])
            else:
                logger.info(message["Body"])
                sqs.delete(sqs_base_url, sqs_output_queue, message["ReceiptHandle"])
        logger.debug("Messages received and deleted from output queue")
    except ClientError as e:
        logger.error(
            f"Failure while retrieving SQS messages, {e.response['Error']['Message']}"
        )


if __name__ == "__main__":
    listen()
