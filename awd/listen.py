import logging

from awd.sqs import SQS

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def listen(sqs_output_queue_url):
    sqs = SQS()
    for message in sqs.receive(sqs_output_queue_url):
        if "'ResultType': 'error'" in message["Body"]:
            logger.error(message["Body"])
            sqs.delete(sqs_output_queue_url, message["ReceiptHandle"])
        else:
            logger.info(message["Body"])
            sqs.delete(sqs_output_queue_url, message["ReceiptHandle"])
    logger.debug("Messages received and deleted from output queue")
