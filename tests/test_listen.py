import logging

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_sqs

from awd import listen

logger = logging.getLogger(__name__)


@mock_sqs
def test_listen_success(
    caplog, sqs_class, result_success_message_body, result_failure_message_body
):
    with caplog.at_level(logging.DEBUG):
        sqs = boto3.resource("sqs", region_name="us-east-1")
        sqs_queue = sqs.create_queue(QueueName="mock-output-queue")
        sqs_class.send(sqs_queue.url, {}, result_failure_message_body)
        sqs_class.send(sqs_queue.url, {}, result_success_message_body)
        listen.listen(sqs_queue.url)
        assert str(result_failure_message_body) in caplog.text
        assert str(result_success_message_body) in caplog.text
        assert "Messages received and deleted from output queue" in caplog.text
        messages = sqs_class.receive(sqs_queue.url)
        assert next(messages, None) is None


def test_listen_failure():
    with pytest.raises(ClientError):
        messages = listen.listen("non-existent")
        for message in messages:
            pass
