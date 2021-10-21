import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_sqs

from awd import sqs


@mock_sqs
def test_sqs_send_success(
    sqs_client, dss_message_attributes_example, dss_message_body_example
):
    sqs = boto3.resource("sqs", region_name="us-east-1")
    sqs_queue = sqs.create_queue(QueueName="mock-queue")
    response = sqs_client.send(
        sqs_queue.url, dss_message_attributes_example, dss_message_body_example
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@mock_sqs
def test_sqs_send_failure(
    sqs_client, dss_message_attributes_example, dss_message_body_example
):
    with pytest.raises(ClientError):
        sqs_client.send(
            "non-existent", dss_message_attributes_example, dss_message_body_example
        )


def test_create_dss_message_attributes(dss_message_attributes_example):
    dss_message_attributes = sqs.create_dss_message_attributes(
        "123", "Submission system", "DSS queue"
    )
    assert dss_message_attributes == dss_message_attributes_example


def test_create_dss_message_body(dss_message_body_example):
    dss_message_body = sqs.create_dss_message_body(
        "DSpace",
        "123.4/5678",
        "mock://bucket/456.json",
        "456.pdf",
        "mock://bucket/456.pdf",
    )
    assert dss_message_body == dss_message_body_example
