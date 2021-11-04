import logging

from awd.sqs import SQS

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def listen(sqs_output_queue_url):
    sqs = SQS()
    for message in sqs.receive(sqs_output_queue_url):
        if '"ResultType": "error"' in message["Body"]:
            logger.error(message["Body"])
        else:
            logger.info(message["Body"])
    return "Messages received from output queue"
