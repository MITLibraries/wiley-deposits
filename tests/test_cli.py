import logging
from http import HTTPStatus

from awd.cli import cli

logger = logging.getLogger(__name__)


def test_deposit_success(
    caplog,
    doi_list_success,
    mocked_web,
    mocked_dynamodb,
    mocked_s3,
    mocked_ses,
    mocked_sqs_input,
    s3_client,
    sqs_client,
    submission_message_body,
    runner,
):
    with caplog.at_level(logging.DEBUG):
        s3_client.put_file(
            file_content=doi_list_success,
            bucket="awd",
            key="doi_success.csv",
        )
        assert len(s3_client.client.list_objects(Bucket="awd")["Contents"]) == 1
        result = runner.invoke(cli, ["deposit"])
        assert result.exit_code == 0
        uploaded_metadata = s3_client.client.get_object(
            Bucket="awd", Key="10.1002-term.3131.json"
        )
        assert uploaded_metadata["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
        s3_client.client.get_object(Bucket="awd", Key="10.1002-term.3131.pdf")
        assert uploaded_metadata["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
        sqs_client.queue_name = "mock-input-queue"
        messages = sqs_client.receive()
        for message in messages:
            assert message["Body"] == submission_message_body
        assert (
            len(s3_client.client.list_objects(Bucket="awd")["Contents"])
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
    mocked_sqs_input,
    s3_client,
    runner,
):
    with caplog.at_level(logging.DEBUG):
        s3_client.put_file(
            file_content=doi_list_insufficient_metadata,
            bucket="awd",
            key="doi_insufficient_metadata.csv",
        )
        result = runner.invoke(cli, ["deposit"])
        assert result.exit_code == 0
        assert (
            "Insufficient metadata for 10.1002/nome.tadata, missing title or URL"
            in caplog.text
        )
        assert len(s3_client.client.list_objects(Bucket="awd")["Contents"]) == 1
        assert "Submission process has completed" in caplog.text
        assert "Logs sent to" in caplog.text


def test_deposit_pdf_unavailable(
    caplog,
    doi_list_pdf_unavailable,
    mocked_web,
    mocked_dynamodb,
    mocked_s3,
    mocked_ses,
    mocked_sqs_input,
    s3_client,
    runner,
):
    with caplog.at_level(logging.DEBUG):
        s3_client.put_file(
            file_content=doi_list_pdf_unavailable,
            bucket="awd",
            key="doi_pdf_unavailable.csv",
        )
        result = runner.invoke(cli, ["deposit"])
        assert result.exit_code == 0
        assert "A PDF could not be retrieved for DOI: 10.1002/none.0000" in caplog.text
        assert len(s3_client.client.list_objects(Bucket="awd")["Contents"]) == 1
        assert "Submission process has completed" in caplog.text
        assert "Logs sent to" in caplog.text


def test_deposit_s3_nonexistent_bucket(
    caplog,
    monkeypatch,
    mocked_web,
    mocked_dynamodb,
    mocked_s3,
    mocked_ses,
    mocked_sqs_input,
    s3_client,
    runner,
):
    with caplog.at_level(logging.DEBUG):
        monkeypatch.setenv("BUCKET", "not-a-bucket")
        result = runner.invoke(cli, ["deposit"])
        assert result.exit_code == 0
        assert (
            "Error accessing bucket: not-a-bucket, The specified bucket does not exist"
        ) in caplog.text


def test_deposit_dynamodb_error(
    caplog,
    monkeypatch,
    doi_list_success,
    mocked_web,
    mocked_dynamodb,
    mocked_s3,
    mocked_ses,
    mocked_sqs_input,
    s3_client,
    runner,
):
    with caplog.at_level(logging.INFO):
        monkeypatch.setenv("DOI_TABLE", "not-a-table")
        s3_client.put_file(
            file_content=doi_list_success,
            bucket="awd",
            key="doi_success.csv",
        )
        result = runner.invoke(cli, ["deposit"])
        assert result.exit_code == 0
        assert ("Unable to read DynamoDB table") in caplog.text


def test_listen_success(
    caplog,
    mocked_dynamodb,
    mocked_s3,
    mocked_ses,
    mocked_sqs_output,
    sample_doiprocessattempt,
    sqs_client,
    result_message_attributes_error,
    result_message_attributes_success,
    result_message_body_error,
    result_message_body_success,
    runner,
):
    with caplog.at_level(logging.DEBUG):
        sqs_client.send(
            result_message_attributes_error,
            result_message_body_error,
        )
        sqs_client.send(
            message_attributes=result_message_attributes_success,
            message_body=result_message_body_success,
        )
        sample_doiprocessattempt.add_item("111.1/1111")
        sample_doiprocessattempt.add_item("222.2/2222")
        result = runner.invoke(cli, ["listen"])
        assert result.exit_code == 0
        assert str(result_message_body_error) in caplog.text
        assert str(result_message_body_success) in caplog.text
        assert "Messages received and deleted from output queue" in caplog.text
        messages = sqs_client.receive()
        assert next(messages, None) is None
        assert "Logs sent to" in caplog.text


def test_listen_message_error(
    caplog, mocked_dynamodb, mocked_ses, mocked_sqs_output, runner, sqs_client
):
    with caplog.at_level(logging.DEBUG):
        sqs_client.send(
            message_attributes={},
            message_body={},
        )
        result = runner.invoke(cli, ["listen"])
        assert result.exit_code == 0
        assert "Error while processing SQS message:" in caplog.text
        assert "Logs sent to" in caplog.text
