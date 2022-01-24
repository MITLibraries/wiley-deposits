import logging

import boto3
from moto import mock_dynamodb2, mock_ses, mock_sqs

from awd.cli import cli, doi_to_be_added, doi_to_be_retried
from awd.status import Status

logger = logging.getLogger(__name__)


def test_doi_to_be_added_true():
    doi_items = [{"doi": "111.1/111"}]
    validation_status = doi_to_be_added("222.2/2222", doi_items)
    assert validation_status is True


def test_doi_to_be_added_false():
    doi_items = [{"doi": "111.1/1111"}]
    validation_status = doi_to_be_added("111.1/1111", doi_items)
    assert validation_status is False


def test_doi_to_be_retried_true():
    doi_items = [{"doi": "111.1/111", "status": str(Status.FAILED.value)}]
    validation_status = doi_to_be_retried("111.1/111", doi_items)
    assert validation_status is True


def test_doi_to_be_retried_false():
    doi_items = [{"doi": "111.1/111", "status": str(Status.SUCCESS.value)}]
    validation_status = doi_to_be_retried("111.1/111", doi_items)
    assert validation_status is False


@mock_dynamodb2
@mock_ses
@mock_sqs
def test_deposit_success(
    caplog, web_mock, s3_mock, s3_class, sqs_class, submission_message_body, runner
):
    with caplog.at_level(logging.DEBUG):
        s3_class.put_file(
            open("tests/fixtures/doi_success.csv", "rb"),
            "awd",
            "doi_success.csv",
        )
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
        assert len(s3_class.client.list_objects(Bucket="awd")["Contents"]) == 1
        result = runner.invoke(
            cli,
            [
                "--doi_table",
                "test_dois",
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--sqs_output_queue",
                "mock-output-queue",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
                "deposit",
                "--metadata_url",
                "http://example.com/works/",
                "--content_url",
                "http://example.com/doi/",
                "--bucket",
                "awd",
                "--sqs_input_queue",
                "mock-input-queue",
                "--collection_handle",
                "123.4/5678",
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
        assert len(s3_class.client.list_objects(Bucket="awd")["Contents"]) == 3
        assert "Submission process has completed" in caplog.text
        assert "Logs sent to" in caplog.text


@mock_dynamodb2
@mock_ses
def test_deposit_insufficient_metadata(caplog, web_mock, s3_mock, s3_class, runner):
    with caplog.at_level(logging.DEBUG):
        s3_class.put_file(
            open("tests/fixtures/doi_insufficient_metadata.csv", "rb"),
            "awd",
            "doi_insufficient_metadata.csv",
        )
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
            cli,
            [
                "--doi_table",
                "test_dois",
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--sqs_output_queue",
                "mock-output-queue",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
                "deposit",
                "--metadata_url",
                "http://example.com/works/",
                "--content_url",
                "http://example.com/doi/",
                "--bucket",
                "awd",
                "--sqs_input_queue",
                "mock-input-queue",
                "--collection_handle",
                "123.4/5678",
            ],
        )
        assert result.exit_code == 0
        assert (
            "Insufficient metadata for 10.1002/nome.tadata, missing title or URL"
            in caplog.text
        )
        assert len(s3_class.client.list_objects(Bucket="awd")["Contents"]) == 1
        assert "Submission process has completed" in caplog.text
        assert "Logs sent to" in caplog.text


@mock_dynamodb2
@mock_ses
def test_deposit_pdf_unavailable(caplog, web_mock, s3_mock, s3_class, runner):
    with caplog.at_level(logging.DEBUG):
        s3_class.put_file(
            open("tests/fixtures/doi_pdf_unavailable.csv", "rb"),
            "awd",
            "doi_pdf_unavailable.csv",
        )
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
            cli,
            [
                "--doi_table",
                "test_dois",
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--sqs_output_queue",
                "mock-output-queue",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
                "deposit",
                "--metadata_url",
                "http://example.com/works/",
                "--content_url",
                "http://example.com/doi/",
                "--bucket",
                "awd",
                "--sqs_input_queue",
                "mock-input-queue",
                "--collection_handle",
                "123.4/5678",
            ],
        )
        assert result.exit_code == 0
        assert "A PDF could not be retrieved for DOI: 10.1002/none.0000" in caplog.text
        assert len(s3_class.client.list_objects(Bucket="awd")["Contents"]) == 1
        assert "Submission process has completed" in caplog.text
        assert "Logs sent to" in caplog.text


@mock_dynamodb2
@mock_ses
def test_deposit_s3_nonexistent_bucket(caplog, web_mock, s3_mock, s3_class, runner):
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
            cli,
            [
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--sqs_output_queue",
                "mock-output-queue",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
                "--doi_table",
                "test_dois",
                "deposit",
                "--metadata_url",
                "http://example.com/works/",
                "--content_url",
                "http://example.com/doi/",
                "--bucket",
                "not-a-bucket",
                "--sqs_input_queue",
                "mock-input-queue",
                "--collection_handle",
                "123.4/5678",
            ],
        )
        assert result.exit_code == 1
        assert (
            "Error accessing bucket: not-a-bucket, The specified bucket does not exist"
        ) in caplog.text


@mock_dynamodb2
@mock_ses
@mock_sqs
def test_listen_success(
    caplog,
    sqs_class,
    result_failure_message_attributes,
    result_success_message_attributes,
    result_failure_message_body,
    result_success_message_body,
    runner,
):
    with caplog.at_level(logging.DEBUG):
        sqs = boto3.resource("sqs", region_name="us-east-1")
        sqs.create_queue(QueueName="mock-output-queue")
        ses_client = boto3.client("ses", region_name="us-east-1")
        ses_client.verify_email_identity(EmailAddress="noreply@example.com")
        sqs_class.send(
            "https://queue.amazonaws.com/123456789012/",
            "mock-output-queue",
            result_failure_message_attributes,
            result_failure_message_body,
        )
        sqs_class.send(
            "https://queue.amazonaws.com/123456789012/",
            "mock-output-queue",
            result_success_message_attributes,
            result_success_message_body,
        )
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
        dynamodb.put_item(
            TableName="test_dois",
            Item={
                "doi": {"S": "111.1/1111"},
                "status": {"S": str(Status.PROCESSING.value)},
                "attempts": {"S": "1"},
            },
        )
        dynamodb.put_item(
            TableName="test_dois",
            Item={
                "doi": {"S": "222.2/2222"},
                "status": {"S": str(Status.PROCESSING.value)},
                "attempts": {"S": "1"},
            },
        )
        result = runner.invoke(
            cli,
            [
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--doi_table",
                "test_dois",
                "--sqs_output_queue",
                "mock-output-queue",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
                "listen",
                "--retry_threshold",
                "10",
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


@mock_dynamodb2
@mock_ses
@mock_sqs
def test_listen_failure(caplog, runner):
    with caplog.at_level(logging.DEBUG):
        ses_client = boto3.client("ses", region_name="us-east-1")
        ses_client.verify_email_identity(EmailAddress="noreply@example.com")
        result = runner.invoke(
            cli,
            [
                "--sqs_base_url",
                "https://queue.amazonaws.com/123456789012/",
                "--doi_table",
                "test_dois",
                "--sqs_output_queue",
                "non-existent",
                "--log_source_email",
                "noreply@example.com",
                "--log_recipient_email",
                "mock@mock.mock",
                "listen",
                "--retry_threshold",
                "10",
            ],
        )
        assert result.exit_code == 0
        assert "Failure while retrieving SQS messages" in caplog.text
        assert "Logs sent to" in caplog.text
