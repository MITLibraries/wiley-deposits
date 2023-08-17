import json
import logging
from http import HTTPStatus

from awd.cli import (
    cli,
    create_list_of_dspace_item_files,
)
from awd.status import Status

logger = logging.getLogger(__name__)


def test_create_list_of_dspace_item_files():
    metadata_content = json.dumps({"key": "value"})
    file_list = create_list_of_dspace_item_files("111.1-111", metadata_content, b"")
    assert file_list == [
        ("111.1-111.json", metadata_content),
        ("111.1-111.pdf", b""),
    ]


def test_deposit_success(
    caplog,
    doi_list_success,
    mocked_web,
    mocked_dynamodb,
    mocked_s3,
    mocked_ses,
    mocked_sqs,
    s3_class,
    sqs_class,
    submission_message_body,
    runner,
):
    with caplog.at_level(logging.DEBUG):
        s3_class.put_file(
            doi_list_success,
            "awd",
            "doi_success.csv",
        )
        assert len(s3_class.client.list_objects(Bucket="awd")["Contents"]) == 1
        result = runner.invoke(
            cli,
            [
                "--log_level",
                "INFO",
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
        assert uploaded_metadata["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
        s3_class.client.get_object(Bucket="awd", Key="10.1002-term.3131.pdf")
        assert uploaded_metadata["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
        messages = sqs_class.receive(
            "https://queue.amazonaws.com/123456789012/", "mock-input-queue"
        )
        for message in messages:
            assert message["Body"] == submission_message_body
        assert (
            len(s3_class.client.list_objects(Bucket="awd")["Contents"])
            == 3  # noqa: PLR2004
        )
        assert "Submission process has completed" in caplog.text
        assert "Logs sent to" in caplog.text


def test_deposit_insufficient_metadata(
    caplog,
    doi_list_insufficient_metadata,
    mocked_web,
    mocked_dynamodb,
    mocked_s3,
    mocked_ses,
    mocked_sqs,
    s3_class,
    runner,
):
    with caplog.at_level(logging.DEBUG):
        s3_class.put_file(
            doi_list_insufficient_metadata,
            "awd",
            "doi_insufficient_metadata.csv",
        )
        result = runner.invoke(
            cli,
            [
                "--log_level",
                "INFO",
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


def test_deposit_pdf_unavailable(
    caplog,
    doi_list_pdf_unavailable,
    mocked_web,
    mocked_dynamodb,
    mocked_s3,
    mocked_ses,
    mocked_sqs,
    s3_class,
    runner,
):
    with caplog.at_level(logging.DEBUG):
        s3_class.put_file(
            doi_list_pdf_unavailable,
            "awd",
            "doi_pdf_unavailable.csv",
        )
        result = runner.invoke(
            cli,
            [
                "--log_level",
                "INFO",
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


def test_deposit_s3_nonexistent_bucket(
    caplog,
    mocked_web,
    mocked_dynamodb,
    mocked_s3,
    mocked_ses,
    mocked_sqs,
    s3_class,
    runner,
):
    with caplog.at_level(logging.DEBUG):
        result = runner.invoke(
            cli,
            [
                "--log_level",
                "INFO",
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
        assert result.exit_code == 0
        assert (
            "Error accessing bucket: not-a-bucket, The specified bucket does not exist"
        ) in caplog.text


def test_listen_success(
    caplog,
    mocked_dynamodb,
    mocked_s3,
    mocked_ses,
    mocked_sqs,
    dynamodb_class,
    sqs_class,
    result_failure_message_attributes,
    result_success_message_attributes,
    result_failure_message_body,
    result_success_message_body,
    runner,
):
    with caplog.at_level(logging.DEBUG):
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
        dynamodb_class.client.put_item(
            TableName="test_dois",
            Item={
                "doi": {"S": "111.1/1111"},
                "status": {"S": str(Status.UNPROCESSED.value)},
                "attempts": {"S": "1"},
                "last_modified": {"S": "'2022-01-28 09:28:53"},
            },
        )
        dynamodb_class.client.put_item(
            TableName="test_dois",
            Item={
                "doi": {"S": "222.2/2222"},
                "status": {"S": str(Status.UNPROCESSED.value)},
                "attempts": {"S": "1"},
                "last_modified": {"S": "'2022-01-28 10:28:53"},
            },
        )
        result = runner.invoke(
            cli,
            [
                "--log_level",
                "INFO",
                "--doi_table",
                "test_dois",
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


def test_listen_failure(caplog, mocked_ses, mocked_sqs, runner):
    with caplog.at_level(logging.DEBUG):
        result = runner.invoke(
            cli,
            [
                "--log_level",
                "INFO",
                "--doi_table",
                "test_dois",
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
