import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_sqs

from awd import sqs


@mock_sqs
def test_sqs_receive_success(
    sqs_client, result_message_attributes, result_message_body
):
    sqs = boto3.resource("sqs", region_name="us-east-1")
    sqs_queue = sqs.create_queue(QueueName="mock-output-queue")
    sqs_client.send(sqs_queue.url, {}, result_message_body)
    messages = sqs_client.receive(sqs_queue.url)
    for message in messages:
        assert message["Body"] == str(result_message_body)


@mock_sqs
def test_sqs_receive_failure(sqs_client):
    with pytest.raises(ClientError):
        messages = sqs_client.receive("non-existent")
        for message in messages:
            pass


@mock_sqs
def test_sqs_send_success(
    sqs_client, submission_message_attributes, submission_message_body
):
    sqs = boto3.resource("sqs", region_name="us-east-1")
    sqs_queue = sqs.create_queue(QueueName="mock-input-queue")
    response = sqs_client.send(
        sqs_queue.url, submission_message_attributes, submission_message_body
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@mock_sqs
def test_sqs_send_failure(
    sqs_client, submission_message_attributes, submission_message_body
):
    with pytest.raises(ClientError):
        sqs_client.send(
            "non-existent", submission_message_attributes, submission_message_body
        )


def test_create_dss_message_attributes(submission_message_attributes):
    dss_message_attributes = sqs.create_dss_message_attributes(
        "123", "Submission system", "DSS queue"
    )
    assert dss_message_attributes == submission_message_attributes


def test_create_dss_message_body(submission_message_body):
    dss_message_body = sqs.create_dss_message_body(
        "DSpace",
        "123.4/5678",
        "mock://bucket/456.json",
        "456.pdf",
        "mock://bucket/456.pdf",
    )
    assert dss_message_body == submission_message_body
