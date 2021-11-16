import logging

import boto3
from moto import mock_sqs

from awd import listen

logger = logging.getLogger(__name__)


@mock_sqs
def test_listen_success(
    caplog, sqs_class, result_success_message_body, result_failure_message_body
):
    with caplog.at_level(logging.DEBUG):
        sqs = boto3.resource("sqs", region_name="us-east-1")
        sqs.create_queue(QueueName="mock-output-queue")
        sqs_class.send(
            "https://queue.amazonaws.com/123456789012/",
            "mock-output-queue",
            {},
            result_failure_message_body,
        )
        sqs_class.send(
            "https://queue.amazonaws.com/123456789012/",
            "mock-output-queue",
            {},
            result_success_message_body,
        )
        listen.listen(
            "https://queue.amazonaws.com/123456789012/",
            "mock-output-queue",
        )
        assert str(result_failure_message_body) in caplog.text
        assert str(result_success_message_body) in caplog.text
        assert "Messages received and deleted from output queue" in caplog.text
        messages = sqs_class.receive(
            "https://queue.amazonaws.com/123456789012/",
            "mock-output-queue",
        )
        assert next(messages, None) is None


@mock_sqs
def test_listen_failure(caplog):
    with caplog.at_level(logging.DEBUG):
        listen.listen("https://queue.amazonaws.com/123456789012/", "non-existent")
        assert "Failure while retrieving SQS messages" in caplog.text
