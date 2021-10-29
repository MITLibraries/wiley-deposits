import logging

import boto3
from moto import mock_sqs

from awd import deposit

logger = logging.getLogger(__name__)


@mock_sqs
def test_deposit_success(
    web_mock, s3_mock, s3_class, sqs_class, submission_message_body
):
    sqs = boto3.resource("sqs", region_name="us-east-1")
    sqs.create_queue(QueueName="mock-input-queue")
    response = deposit.deposit(
        "tests/fixtures/doi_success.csv",
        "http://example.com/works/",
        "http://example.com/doi/",
        "awd",
        "https://queue.amazonaws.com/123456789012/mock-input-queue",
        "https://queue.amazonaws.com/123456789012/mock-output-queue",
        "123.4/5678",
    )
    uploaded_metadata = s3_class.client.get_object(
        Bucket="awd", Key="10.1002-term.3131.json"
    )
    assert uploaded_metadata["ResponseMetadata"]["HTTPStatusCode"] == 200
    uploaded_bitstream = s3_class.client.get_object(
        Bucket="awd", Key="10.1002-term.3131.pdf"
    )
    assert uploaded_bitstream["ResponseMetadata"]["HTTPStatusCode"] == 200
    messages = sqs_class.receive(
        "https://queue.amazonaws.com/123456789012/mock-input-queue"
    )
    for message in messages:
        assert message["Body"] == str(submission_message_body)
    assert response == "Submission process has completed"


def test_deposit_insufficient_metadata(caplog, web_mock, s3_mock, s3_class):
    with caplog.at_level(logging.INFO):
        response = deposit.deposit(
            "tests/fixtures/doi_insufficient_metadata.csv",
            "http://example.com/works/",
            "http://example.com/doi/",
            "awd",
            "mock-input-queue",
            "mock-output-queue",
            "123.4/5678",
        )
        assert (
            "Insufficient metadata for 10.1002/nome.tadata, missing title or URL"
            in caplog.text
        )
        assert "Contents" not in s3_class.client.list_objects(Bucket="awd")
        assert response == "Submission process has completed"


def test_deposit_pdf_unavailable(caplog, web_mock, s3_mock, s3_class):
    with caplog.at_level(logging.INFO):
        response = deposit.deposit(
            "tests/fixtures/doi_pdf_unavailable.csv",
            "http://example.com/works/",
            "http://example.com/doi/",
            "awd",
            "mock-input-queue",
            "mock-output-queue",
            "123.4/5678",
        )
        assert "A PDF could not be retrieved for DOI: 10.1002/none.0000" in caplog.text
        assert "Contents" not in s3_class.client.list_objects(Bucket="awd")
        assert response == "Submission process has completed"


def test_deposit_s3_upload_failed(caplog, web_mock, s3_mock, s3_class):
    with caplog.at_level(logging.INFO):
        response = deposit.deposit(
            "tests/fixtures/doi_success.csv",
            "http://example.com/works/",
            "http://example.com/doi/",
            "not-a-bucket",
            "mock-input-queue",
            "mock-output-queue",
            "123.4/5678",
        )
    assert "Upload failed: 10.1002-term.3131.json" in caplog.text
    assert "Contents" not in s3_class.client.list_objects(Bucket="awd")
    assert response == "Submission process has completed"
