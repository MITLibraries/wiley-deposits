import logging

from botocore.exceptions import ClientError

from awd.sqs import SQS

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


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
