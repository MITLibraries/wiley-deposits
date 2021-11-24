import logging

import boto3
from moto import mock_ses, mock_sqs

from awd.listen import listen

logger = logging.getLogger(__name__)


@mock_ses
@mock_sqs
def test_listen_success(
    caplog, sqs_class, result_success_message_body, result_failure_message_body, runner
):
    with caplog.at_level(logging.DEBUG):
        sqs = boto3.resource("sqs", region_name="us-east-1")
        sqs.create_queue(QueueName="mock-output-queue")
        ses_client = boto3.client("ses", region_name="us-east-1")
        ses_client.verify_email_identity(EmailAddress="noreply@example.com")
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
        result = runner.invoke(
            listen,
            [
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--sqs_output_queue",
                "mock-output-queue",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
            ],
        )
        assert result.exit_code == 0
        assert str(result_failure_message_body) in caplog.text
        assert str(result_success_message_body) in caplog.text
        assert "Messages received and deleted from output queue" in caplog.text
        messages = sqs_class.receive(
            "https://queue.amazonaws.com/123456789012/", "mock-output-queue"
        )
        assert next(messages, None) is None
        assert "Logs sent to" in caplog.text


@mock_ses
@mock_sqs
def test_listen_failure(caplog, runner):
    with caplog.at_level(logging.DEBUG):
        ses_client = boto3.client("ses", region_name="us-east-1")
        ses_client.verify_email_identity(EmailAddress="noreply@example.com")
        result = runner.invoke(
            listen,
            [
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--sqs_output_queue",
                "non-existent",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
            ],
        )
        assert result.exit_code == 0
        assert "Failure while retrieving SQS messages" in caplog.text
        assert "Logs sent to" in caplog.text
