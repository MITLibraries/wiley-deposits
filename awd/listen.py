import logging

from botocore.exceptions import ClientError

from awd.sqs import SQS

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def listen(sqs_output_queue_url):
    sqs = SQS()
    try:
        for message in sqs.receive(sqs_output_queue_url):
            if "'ResultType': 'error'" in message["Body"]:
                logger.error(message["Body"])
                sqs.delete(sqs_output_queue_url, message["ReceiptHandle"])
            else:
                logger.info(message["Body"])
                sqs.delete(sqs_output_queue_url, message["ReceiptHandle"])
        logger.debug("Messages received and deleted from output queue")
    except ClientError as e:
        logger.error(
            f"Failure while retrieving SQS messages, {e.response['Error']['Message']}"
        )


if __name__ == "__main__":
    listen()
