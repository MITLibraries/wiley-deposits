import logging
from email.mime.multipart import MIMEMultipart
from http import HTTPStatus

import pytest
from botocore.exceptions import ClientError

from awd.helpers import (
    get_crossref_response_from_doi,
    get_dois_from_spreadsheet,
    get_wiley_response,
)


def test_s3_filter_files_in_bucket_with_matching_csv(mocked_s3, s3_client):
    s3_client.put_file(
        "test1,test2,test3,test4",
        "awd",
        "test.csv",
    )
    assert list(s3_client.filter_files_in_bucket("awd", "csv", "archived")) == [
        "test.csv"
    ]


def test_s3_filter_files_in_bucket_without_matching_csv(mocked_s3, s3_client):
    s3_client.put_file(
        "test1,test2,test3,test4",
        "awd",
        "archived/test.csv",
    )
    assert list(s3_client.filter_files_in_bucket("awd", "csv", "archived")) == []


def test_s3_archive_file_in_bucket(mocked_s3, s3_client):
    s3_client.put_file(
        "test1,test2,test3,test4",
        "awd",
        "test.csv",
    )
    s3_client.archive_file_with_new_key(
        "awd",
        "test.csv",
        "archived",
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
        str({"metadata": {"key": "dc.title", "value": "A Title"}}),
        "awd",
        "test.json",
    )
    assert len(s3_client.client.list_objects(Bucket="awd")["Contents"]) == 1
    assert (
        s3_client.client.list_objects(Bucket="awd")["Contents"][0]["Key"] == "test.json"
    )


def test_ses_create_email(ses_client):
    message = ses_client.create_email(
        "Email subject",
        "<html/>",
        "attachment",
    )
    assert message["Subject"] == "Email subject"
    assert message.get_payload()[0].get_filename() == "attachment"


def test_ses_send_email(mocked_ses, ses_client):
    message = MIMEMultipart()
    response = ses_client.send_email(
        "noreply@example.com",
        "test@example.com",
        message,
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


def test_sqs_create_dss_message_attributes(sqs_client, submission_message_attributes):
    dss_message_attributes = sqs_client.create_dss_message_attributes(
        "123", "Submission system", "DSS queue"
    )
    assert dss_message_attributes == submission_message_attributes


def test_sqs_create_dss_message_body(sqs_client, submission_message_body):
    dss_message_body = sqs_client.create_dss_message_body(
        "DSpace@MIT",
        "123.4/5678",
        "s3://awd/10.1002-term.3131.json",
        "10.1002-term.3131.pdf",
        "s3://awd/10.1002-term.3131.pdf",
    )
    assert dss_message_body == submission_message_body


def test_sqs_delete_success(
    mocked_sqs,
    sqs_client,
    result_success_message_attributes,
    result_success_message_body,
):
    sqs_client.send(
        "https://queue.amazonaws.com/123456789012/",
        "mock-output-queue",
        result_success_message_attributes,
        result_success_message_body,
    )
    messages = sqs_client.receive(
        "https://queue.amazonaws.com/123456789012/", "mock-output-queue"
    )
    receipt_handle = next(messages)["ReceiptHandle"]
    response = sqs_client.delete(
        "https://queue.amazonaws.com/123456789012/", "mock-output-queue", receipt_handle
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_sqs_delete_failure(mocked_sqs, sqs_client):
    with pytest.raises(ClientError):
        sqs_client.delete(
            "https://queue.amazonaws.com/123456789012/", "non-existent", "12345678"
        )


def test_sqs_receive_success(
    mocked_sqs,
    sqs_client,
    result_success_message_attributes,
    result_success_message_body,
):
    sqs_client.send(
        "https://queue.amazonaws.com/123456789012/",
        "mock-output-queue",
        result_success_message_attributes,
        result_success_message_body,
    )
    messages = sqs_client.receive(
        "https://queue.amazonaws.com/123456789012/", "mock-output-queue"
    )
    for message in messages:
        assert message["Body"] == str(result_success_message_body)
        assert message["MessageAttributes"] == result_success_message_attributes


def test_sqs_receive_failure(mocked_sqs, sqs_client):
    with pytest.raises(ClientError):
        next(
            sqs_client.receive(
                "https://queue.amazonaws.com/123456789012/", "non-existent"
            )
        )


def test_sqs_send_success(
    mocked_sqs, sqs_client, submission_message_attributes, submission_message_body
):
    response = sqs_client.send(
        "https://queue.amazonaws.com/123456789012/",
        "mock-input-queue",
        submission_message_attributes,
        submission_message_body,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_sqs_send_failure(
    mocked_sqs, sqs_client, submission_message_attributes, submission_message_body
):
    with pytest.raises(ClientError):
        sqs_client.send(
            "https://queue.amazonaws.com/123456789012/",
            "non-existent",
            submission_message_attributes,
            submission_message_body,
        )


def test_get_dois_from_spreadsheet():
    dois = get_dois_from_spreadsheet("tests/fixtures/doi_success.csv")
    for doi in dois:
        assert doi == "10.1002/term.3131"


def test_get_crossref_work_from_doi(mocked_web):
    response = get_crossref_response_from_doi(
        "http://example.com/works/", "10.1002/term.3131"
    )
    work = response.json()
    assert work["message"]["title"] == ["Metal nanoparticles for bone tissue engineering"]


def test_get_wiley_response(mocked_web, wiley_pdf):
    doi = "10.1002/term.3131"
    response = get_wiley_response("http://example.com/doi/", doi)
    assert response.content == wiley_pdf
