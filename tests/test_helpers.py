import logging
from email.mime.multipart import MIMEMultipart
from http import HTTPStatus

import pytest
from botocore.exceptions import ClientError

from awd.helpers import (
    InvalidSQSMessageError,
    get_crossref_response_from_doi,
    get_dois_from_spreadsheet,
    get_wiley_response,
)
from awd.status import Status


# S3Client tests
def test_s3_archive_file_in_bucket(mocked_s3, s3_client):
    s3_client.put_file(
        file_content="test1,test2,test3,test4",
        bucket="awd",
        key="test.csv",
    )
    s3_client.archive_file_with_new_key(
        bucket="awd",
        key="test.csv",
        archived_key_prefix="archived",
    )
    with pytest.raises(ClientError) as e:
        response = s3_client.client.get_object(Bucket="awd", Key="test.csv")
    assert (
        "An error occurred (NoSuchKey) when calling the GetObject operation: The"
        " specified key does not exist." in str(e.value)
    )
    response = s3_client.client.get_object(Bucket="awd", Key="archived/test.csv")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_s3_put_file(mocked_s3, s3_client):
    assert "Contents" not in s3_client.client.list_objects(Bucket="awd")
    s3_client.put_file(
        file_content=str({"metadata": {"key": "dc.title", "value": "A Title"}}),
        bucket="awd",
        key="test.json",
    )
    assert len(s3_client.client.list_objects(Bucket="awd")["Contents"]) == 1
    assert (
        s3_client.client.list_objects(Bucket="awd")["Contents"][0]["Key"] == "test.json"
    )


def test_s3_retrieve_file_type_from_bucket_with_matching_csv(mocked_s3, s3_client):
    s3_client.put_file(
        file_content="test1,test2,test3,test4",
        bucket="awd",
        key="test.csv",
    )
    assert list(
        s3_client.retrieve_file_type_from_bucket(
            bucket="awd", file_type="csv", excluded_key_prefix="archived"
        )
    ) == ["test.csv"]


def test_s3_retrieve_file_type_from_bucket_without_matching_csv(mocked_s3, s3_client):
    s3_client.put_file(
        file_content="test1,test2,test3,test4",
        bucket="awd",
        key="archived/test.csv",
    )
    assert (
        list(
            s3_client.retrieve_file_type_from_bucket(
                bucket="awd", file_type="csv", excluded_key_prefix="archived"
            )
        )
        == []
    )


# SESClient tests
def test_ses_create_email(ses_client):
    message = ses_client.create_email(
        subject="Email subject",
        attachment_content="<html/>",
        attachment_name="attachment",
    )
    assert message["Subject"] == "Email subject"
    assert message.get_payload()[0].get_filename() == "attachment"


def test_ses_send_email(mocked_ses, ses_client):
    message = MIMEMultipart()
    response = ses_client.send_email(
        source_email_address="noreply@example.com",
        recipient_email_address="test@example.com",
        message=message,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_ses_create_and_send_email(caplog, mocked_ses, ses_client):
    with caplog.at_level(logging.DEBUG):
        ses_client.create_and_send_email(
            subject="Email subject",
            attachment_content="<html/>",
            attachment_name="attachment",
            source_email_address="noreply@example.com",
            recipient_email_address="test@example.com",
        )
        assert "Logs sent to test@example.com" in caplog.text


# SQSClient tests
def test_sqs_create_dss_message_attributes(sqs_client, submission_message_attributes):
    dss_message_attributes = sqs_client.create_dss_message_attributes(
        package_id="123", submission_source="Submission system", output_queue="DSS queue"
    )
    assert dss_message_attributes == submission_message_attributes


def test_sqs_create_dss_message_body(sqs_client, submission_message_body):
    dss_message_body = sqs_client.create_dss_message_body(
        submission_system="DSpace@MIT",
        collection_handle="123.4/5678",
        metadata_s3_uri="s3://awd/10.1002-term.3131.json",
        bitstream_file_name="10.1002-term.3131.pdf",
        bitstream_s3_uri="s3://awd/10.1002-term.3131.pdf",
    )
    assert dss_message_body == submission_message_body


def test_sqs_delete_nonexistent_message_raises_error(mocked_sqs_output, sqs_client):
    with pytest.raises(ClientError):
        sqs_client.delete(receipt_handle="12345678")


def test_sqs_delete_success(
    mocked_sqs_output,
    sqs_client,
    result_message_attributes_error,
    result_message_body_error,
):
    sqs_client.send(
        message_attributes=result_message_attributes_error,
        message_body=result_message_body_error,
    )
    messages = sqs_client.receive()
    receipt_handle = next(messages)["ReceiptHandle"]
    response = sqs_client.delete(receipt_handle=receipt_handle)
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_sqs_error_message_above_retry_threshold_set_failed_status(
    caplog,
    mocked_sqs_output,
    result_message_body_error,
    result_message_attributes_error,
    sample_doiprocessattempt,
    sqs_client,
):
    sqs_client.send(
        message_attributes=result_message_attributes_error,
        message_body=result_message_body_error,
    )
    messages = sqs_client.receive()
    receipt_handle = next(messages)["ReceiptHandle"]
    sample_doiprocessattempt.doi = "222.2/2222"
    sqs_client.error_message(
        doi="222.2/2222",
        message_body=result_message_body_error,
        receipt_handle=receipt_handle,
        doi_process_attempt=sample_doiprocessattempt,
        retry_threshold="0",
    )
    assert sample_doiprocessattempt.get("222.2/2222").status == Status.FAILED.value
    assert (
        "DOI: '222.2/2222' has exceeded the retry threshold and will not be "
        "attempted again" in caplog.text
    )


def test_sqs_error_message_below_retry_threshold_set_unprocessed_status(
    caplog,
    mocked_sqs_output,
    result_message_body_error,
    result_message_attributes_error,
    sample_doiprocessattempt,
    sqs_client,
):
    sqs_client.send(
        message_attributes=result_message_attributes_error,
        message_body=result_message_body_error,
    )
    messages = sqs_client.receive()
    receipt_handle = next(messages)["ReceiptHandle"]
    sample_doiprocessattempt.doi = "222.2/2222"
    sqs_client.error_message(
        doi="222.2/2222",
        message_body=result_message_body_error,
        receipt_handle=receipt_handle,
        doi_process_attempt=sample_doiprocessattempt,
        retry_threshold="30",
    )
    assert sample_doiprocessattempt.get("222.2/2222").status == Status.UNPROCESSED.value
    assert (
        'DOI: 222.2/2222, Result: {"ResultType": "error", "ErrorTimestamp": '
        '"Thu Sep 09 18:32:39 UTC 2021", "ErrorInfo": "Error occurred while posting '
        'item to DSpace", "ExceptionMessage": "500 Server Error: Internal Server Error", '
        '"ExceptionTraceback": "Full unformatted stack trace of the Exception"}'
        in caplog.text
    )


def test_sqs_process_result_message_error(
    mocked_sqs_output,
    sqs_client,
    sample_doiprocessattempt,
    result_message_attributes_error,
    result_message_body_error,
):
    sqs_client.send(
        message_attributes=result_message_attributes_error,
        message_body=result_message_body_error,
    )
    messages = sqs_client.receive()
    sample_doiprocessattempt.doi = "222.2/2222"
    sqs_client.process_result_message(
        sqs_message=next(messages),
        retry_threshold=30,
    )
    assert sample_doiprocessattempt.get("222.2/2222").status == Status.UNPROCESSED.value


def test_sqs_process_result_message_raises_invalid_sqs_exception(
    mocked_sqs_output,
    sqs_client,
    sample_doiprocessattempt,
    result_message_attributes_success,
    result_message_body_success,
):
    sqs_client.send(message_attributes={}, message_body={})
    messages = sqs_client.receive()
    with pytest.raises(InvalidSQSMessageError):
        sqs_client.process_result_message(
            sqs_message=next(messages),
            retry_threshold=30,
        )


def test_sqs_process_result_message_success(
    mocked_sqs_output,
    sqs_client,
    sample_doiprocessattempt,
    result_message_attributes_success,
    result_message_body_success,
):
    sqs_client.send(
        message_attributes=result_message_attributes_success,
        message_body=result_message_body_success,
    )
    messages = sqs_client.receive()
    sqs_client.process_result_message(
        sqs_message=next(messages),
        retry_threshold=30,
    )
    assert (
        sample_doiprocessattempt.get("10.1002/term.3131").status == Status.SUCCESS.value
    )


def test_sqs_receive_raises_error_for_incorrect_queue(mocked_sqs_output, sqs_client):
    sqs_client.queue_name = "non-existent"
    with pytest.raises(ClientError):
        next(sqs_client.receive())


def test_sqs_receive_success(
    mocked_sqs_output,
    sqs_client,
    result_message_attributes_success,
    result_message_body_success,
):
    sqs_client.send(
        message_attributes=result_message_attributes_success,
        message_body=result_message_body_success,
    )
    messages = sqs_client.receive()
    for message in messages:
        assert message["Body"] == str(result_message_body_success)
        assert message["MessageAttributes"] == result_message_attributes_success


def test_sqs_send_raises_error_for_incorrect_queue(
    mocked_sqs_input, sqs_client, submission_message_attributes, submission_message_body
):
    sqs_client.queue_name = "non-existent"
    with pytest.raises(ClientError):
        sqs_client.send(
            message_attributes=submission_message_attributes,
            message_body=submission_message_body,
        )


def test_sqs_send_success(
    mocked_sqs_input, sqs_client, submission_message_attributes, submission_message_body
):
    sqs_client.queue_name = "mock-input-queue"
    response = sqs_client.send(
        message_attributes=submission_message_attributes,
        message_body=submission_message_body,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_sqs_success_message(
    mocked_sqs_output,
    result_message_attributes_success,
    result_message_body_success,
    sample_doiprocessattempt,
    sqs_client,
):
    sqs_client.send(
        message_attributes=result_message_attributes_success,
        message_body=result_message_body_success,
    )
    messages = sqs_client.receive()
    receipt_handle = next(messages)["ReceiptHandle"]
    sqs_client.success_message(
        doi="10.1002/term.3131",
        message_body=result_message_body_success,
        receipt_handle=receipt_handle,
        doi_process_attempt=sample_doiprocessattempt,
    )
    assert (
        sample_doiprocessattempt.get("10.1002/term.3131").status == Status.SUCCESS.value
    )


def test_sqs_valid_result_message_attributes_false(mocked_sqs_input, sqs_client):
    assert not sqs_client.valid_result_message_attributes(sqs_message={})


def test_sqs_valid_result_message_attributes_true(
    mocked_sqs_input, sqs_client, valid_result_message
):
    assert sqs_client.valid_result_message_attributes(sqs_message=valid_result_message)


def test_sqs_valid_result_message_body_false(caplog, mocked_sqs_input, sqs_client):
    assert not sqs_client.valid_result_message_body(sqs_message={None})


def test_sqs_valid_result_message_body_true(
    mocked_sqs_input, sqs_client, valid_result_message
):
    assert sqs_client.valid_result_message_body(sqs_message=valid_result_message)


# Function tests
def test_get_crossref_work_from_doi(mocked_web):
    response = get_crossref_response_from_doi(
        url="http://example.com/works/", doi="10.1002/term.3131"
    )
    work = response.json()
    assert work["message"]["title"] == ["Metal nanoparticles for bone tissue engineering"]


def test_get_dois_from_spreadsheet():
    dois = get_dois_from_spreadsheet(doi_csv_file="tests/fixtures/doi_success.csv")
    for doi in dois:
        assert doi == "10.1002/term.3131"


def test_get_wiley_response(mocked_web, wiley_pdf):
    response = get_wiley_response(url="http://example.com/doi/", doi="10.1002/term.3131")
    assert response.content == wiley_pdf
