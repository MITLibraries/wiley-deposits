import logging

import boto3
from moto import mock_dynamodb2, mock_ses, mock_sqs

from awd.deposit import deposit

logger = logging.getLogger(__name__)


@mock_dynamodb2
@mock_ses
@mock_sqs
def test_deposit_success(
    caplog, web_mock, s3_mock, s3_class, sqs_class, submission_message_body, runner
):
    with caplog.at_level(logging.DEBUG):
        sqs = boto3.resource("sqs", region_name="us-east-1")
        sqs.create_queue(QueueName="mock-input-queue")
        ses_client = boto3.client("ses", region_name="us-east-1")
        ses_client.verify_email_identity(EmailAddress="noreply@example.com")
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName="test_dois",
            KeySchema=[
                {"AttributeName": "doi", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "doi", "AttributeType": "S"},
            ],
        )
        result = runner.invoke(
            deposit,
            [
                "--doi_file_path",
                "tests/fixtures/doi_success.csv",
                "--doi_table",
                "test_dois",
                "--metadata_url",
                "http://example.com/works/",
                "--content_url",
                "http://example.com/doi/",
                "--bucket",
                "awd",
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--sqs_input_queue",
                "mock-input-queue",
                "--sqs_output_queue",
                "mock-output-queue",
                "--collection_handle",
                "123.4/5678",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
            ],
        )
        assert result.exit_code == 0
        uploaded_metadata = s3_class.client.get_object(
            Bucket="awd", Key="10.1002-term.3131.json"
        )
        assert uploaded_metadata["ResponseMetadata"]["HTTPStatusCode"] == 200
        uploaded_bitstream = s3_class.client.get_object(
            Bucket="awd", Key="10.1002-term.3131.pdf"
        )
        assert uploaded_bitstream["ResponseMetadata"]["HTTPStatusCode"] == 200
        messages = sqs_class.receive(
            "https://queue.amazonaws.com/123456789012/", "mock-input-queue"
        )
        for message in messages:
            assert message["Body"] == submission_message_body
        assert "Submission process has completed" in caplog.text
        assert "Logs sent to" in caplog.text


@mock_dynamodb2
@mock_ses
def test_deposit_insufficient_metadata(caplog, web_mock, s3_mock, s3_class, runner):
    with caplog.at_level(logging.DEBUG):
        ses_client = boto3.client("ses", region_name="us-east-1")
        ses_client.verify_email_identity(EmailAddress="noreply@example.com")
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName="test_dois",
            KeySchema=[
                {"AttributeName": "doi", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "doi", "AttributeType": "S"},
            ],
        )
        result = runner.invoke(
            deposit,
            [
                "--doi_file_path",
                "tests/fixtures/doi_insufficient_metadata.csv",
                "--doi_table",
                "test_dois",
                "--metadata_url",
                "http://example.com/works/",
                "--content_url",
                "http://example.com/doi/",
                "--bucket",
                "awd",
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--sqs_input_queue",
                "mock-input-queue",
                "--sqs_output_queue",
                "mock-output-queue",
                "--collection_handle",
                "123.4/5678",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
            ],
        )
        assert result.exit_code == 0
        assert (
            "Insufficient metadata for 10.1002/nome.tadata, missing title or URL"
            in caplog.text
        )
        assert "Contents" not in s3_class.client.list_objects(Bucket="awd")
        assert "Submission process has completed" in caplog.text
        assert "Logs sent to" in caplog.text


@mock_dynamodb2
@mock_ses
def test_deposit_pdf_unavailable(caplog, web_mock, s3_mock, s3_class, runner):
    with caplog.at_level(logging.DEBUG):
        ses_client = boto3.client("ses", region_name="us-east-1")
        ses_client.verify_email_identity(EmailAddress="noreply@example.com")
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName="test_dois",
            KeySchema=[
                {"AttributeName": "doi", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "doi", "AttributeType": "S"},
            ],
        )
        result = runner.invoke(
            deposit,
            [
                "--doi_file_path",
                "tests/fixtures/doi_pdf_unavailable.csv",
                "--doi_table",
                "test_dois",
                "--metadata_url",
                "http://example.com/works/",
                "--content_url",
                "http://example.com/doi/",
                "--bucket",
                "awd",
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--sqs_input_queue",
                "mock-input-queue",
                "--sqs_output_queue",
                "mock-output-queue",
                "--collection_handle",
                "123.4/5678",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
            ],
        )
        assert result.exit_code == 0
        assert "A PDF could not be retrieved for DOI: 10.1002/none.0000" in caplog.text
        assert "Contents" not in s3_class.client.list_objects(Bucket="awd")
        assert "Submission process has completed" in caplog.text
        assert "Logs sent to" in caplog.text


@mock_dynamodb2
@mock_ses
def test_deposit_s3_upload_failed(caplog, web_mock, s3_mock, s3_class, runner):
    with caplog.at_level(logging.DEBUG):
        ses_client = boto3.client("ses", region_name="us-east-1")
        ses_client.verify_email_identity(EmailAddress="noreply@example.com")
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            TableName="test_dois",
            KeySchema=[
                {"AttributeName": "doi", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "doi", "AttributeType": "S"},
            ],
        )
        result = runner.invoke(
            deposit,
            [
                "--doi_file_path",
                "tests/fixtures/doi_success.csv",
                "--doi_table",
                "test_dois",
                "--metadata_url",
                "http://example.com/works/",
                "--content_url",
                "http://example.com/doi/",
                "--bucket",
                "not-a-bucket",
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--sqs_input_queue",
                "mock-input-queue",
                "--sqs_output_queue",
                "mock-output-queue",
                "--collection_handle",
                "123.4/5678",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
            ],
        )
        assert result.exit_code == 0
        assert "Upload failed: 10.1002-term.3131.json" in caplog.text
        assert "Contents" not in s3_class.client.list_objects(Bucket="awd")
        assert "Submission process has completed" in caplog.text
        assert "Logs sent to" in caplog.text
