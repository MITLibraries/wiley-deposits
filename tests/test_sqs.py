import os

import boto3
import pytest
from botocore.exceptions import ClientError
from moto.core import set_initial_no_auth_action_count

from awd import config, sqs


def test_check_read_permissions_success(
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
    result = sqs_class.check_read_permissions(
        "https://queue.amazonaws.com/123456789012/", "mock-output-queue"
    )
    assert result == "SQS read permissions confirmed for queue: mock-output-queue"


@set_initial_no_auth_action_count(1)
def test_check_read_permissions_raises_error_if_no_permission(
    mocked_sqs,
    sqs_class,
    test_aws_user,
    result_success_message_attributes,
    result_success_message_body,
):
    sqs_class.send(
        "https://queue.amazonaws.com/123456789012/",
        "mock-output-queue",
        result_success_message_attributes,
        result_success_message_body,
    )
    os.environ["AWS_ACCESS_KEY_ID"] = test_aws_user["AccessKeyId"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = test_aws_user["SecretAccessKey"]
    boto3.setup_default_session()
    sqs_class = sqs.SQS(config.AWS_REGION_NAME)
    with pytest.raises(ClientError) as e:
        sqs_class.check_read_permissions(
            "https://queue.amazonaws.com/123456789012/", "mock-output-queue"
        )
    assert (
        "User: arn:aws:iam::123456789012:user/test-user is not authorized to perform: "
        "sqs:ReceiveMessage"
    ) in str(e.value)


def test_check_write_permissions_success(mocked_sqs, sqs_class):
    messages = sqs_class.receive(
        "https://queue.amazonaws.com/123456789012/", "mock-input-queue"
    )
    with pytest.raises(StopIteration):
        next(messages)
    result = sqs_class.check_write_permissions(
        "https://queue.amazonaws.com/123456789012/", "mock-input-queue"
    )
    assert result == "SQS write permissions confirmed for queue: mock-input-queue"
    messages = sqs_class.receive(
        "https://queue.amazonaws.com/123456789012/", "mock-input-queue"
    )
    with pytest.raises(StopIteration):
        next(messages)


@set_initial_no_auth_action_count(1)
def test_check_write_permissions_raises_error_if_no_permission(
    mocked_sqs,
    sqs_class,
    test_aws_user,
    result_success_message_attributes,
    result_success_message_body,
):
    sqs_class.send(
        "https://queue.amazonaws.com/123456789012/",
        "mock-output-queue",
        result_success_message_attributes,
        result_success_message_body,
    )
    os.environ["AWS_ACCESS_KEY_ID"] = test_aws_user["AccessKeyId"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = test_aws_user["SecretAccessKey"]
    boto3.setup_default_session()
    sqs_class = sqs.SQS(config.AWS_REGION_NAME)
    with pytest.raises(ClientError) as e:
        sqs_class.check_write_permissions(
            "https://queue.amazonaws.com/123456789012/", "empty_input_queue"
        )
    assert (
        "User: arn:aws:iam::123456789012:user/test-user is not authorized to perform: "
        "sqs:SendMessage"
    ) in str(e.value)


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
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_sqs_delete_failure(sqs_class):
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


def test_sqs_receive_failure(sqs_class):
    with pytest.raises(ClientError):
        messages = sqs_class.receive(
            "https://queue.amazonaws.com/123456789012/", "non-existent"
        )
        for message in messages:
            pass


def test_sqs_send_success(
    mocked_sqs, sqs_class, submission_message_attributes, submission_message_body
):
    response = sqs_class.send(
        "https://queue.amazonaws.com/123456789012/",
        "mock-input-queue",
        submission_message_attributes,
        submission_message_body,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_sqs_send_failure(
    sqs_class, submission_message_attributes, submission_message_body
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
