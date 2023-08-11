import pytest
from botocore.exceptions import ClientError

from awd import sqs
from awd.config import STATUS_CODE_200


def test_sqs_delete_success(
    mocked_sqs,
    sqs_class,
    result_success_message_attributes,
    result_success_message_body,
):
    sqs_class.send(
        "https://queue.amazonaws.com/123456789012/",
        "mock-output-queue",
        result_success_message_attributes,
        result_success_message_body,
    )
    messages = sqs_class.receive(
        "https://queue.amazonaws.com/123456789012/", "mock-output-queue"
    )
    receipt_handle = next(messages)["ReceiptHandle"]
    response = sqs_class.delete(
        "https://queue.amazonaws.com/123456789012/", "mock-output-queue", receipt_handle
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == STATUS_CODE_200


def test_sqs_delete_failure(mocked_sqs, sqs_class):
    with pytest.raises(ClientError):
        sqs_class.delete(
            "https://queue.amazonaws.com/123456789012/", "non-existent", "12345678"
        )


def test_sqs_receive_success(
    mocked_sqs,
    sqs_class,
    result_success_message_attributes,
    result_success_message_body,
):
    sqs_class.send(
        "https://queue.amazonaws.com/123456789012/",
        "mock-output-queue",
        result_success_message_attributes,
        result_success_message_body,
    )
    messages = sqs_class.receive(
        "https://queue.amazonaws.com/123456789012/", "mock-output-queue"
    )
    for message in messages:
        assert message["Body"] == str(result_success_message_body)
        assert message["MessageAttributes"] == result_success_message_attributes


def test_sqs_receive_failure(mocked_sqs, sqs_class):
    with pytest.raises(ClientError):
        next(
            sqs_class.receive("https://queue.amazonaws.com/123456789012/", "non-existent")
        )


def test_sqs_send_success(
    mocked_sqs, sqs_class, submission_message_attributes, submission_message_body
):
    response = sqs_class.send(
        "https://queue.amazonaws.com/123456789012/",
        "mock-input-queue",
        submission_message_attributes,
        submission_message_body,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == STATUS_CODE_200


def test_sqs_send_failure(
    mocked_sqs, sqs_class, submission_message_attributes, submission_message_body
):
    with pytest.raises(ClientError):
        sqs_class.send(
            "https://queue.amazonaws.com/123456789012/",
            "non-existent",
            submission_message_attributes,
            submission_message_body,
        )


def test_create_dss_message_attributes(submission_message_attributes):
    dss_message_attributes = sqs.create_dss_message_attributes(
        "123", "Submission system", "DSS queue"
    )
    assert dss_message_attributes == submission_message_attributes


def test_create_dss_message_body(submission_message_body):
    dss_message_body = sqs.create_dss_message_body(
        "DSpace@MIT",
        "123.4/5678",
        "s3://awd/10.1002-term.3131.json",
        "10.1002-term.3131.pdf",
        "s3://awd/10.1002-term.3131.pdf",
    )
    assert dss_message_body == submission_message_body
