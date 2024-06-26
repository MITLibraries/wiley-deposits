import datetime
import json

import boto3
import pytest
import requests_mock
from click.testing import CliRunner
from freezegun import freeze_time
from moto import mock_aws

from awd import config
from awd.article import Article
from awd.config import Config
from awd.database import DoiProcessAttempt
from awd.helpers import (
    S3Client,
    SESClient,
    SQSClient,
)


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture(autouse=True)
def _test_environment(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("WORKSPACE", "test")
    monkeypatch.setenv("LOG_LEVEL", "INFO")
    monkeypatch.setenv("DOI_TABLE", "wiley-test")
    monkeypatch.setenv("METADATA_URL", "http://example.com/works/")
    monkeypatch.setenv("CONTENT_URL", "http://example.com/doi/")
    monkeypatch.setenv("BUCKET", "awd")
    monkeypatch.setenv("SQS_BASE_URL", "https://queue.amazonaws.com/123456789012/")
    monkeypatch.setenv("SQS_INPUT_QUEUE", "mock-input-queue")
    monkeypatch.setenv("SQS_OUTPUT_QUEUE", "mock-output-queue")
    monkeypatch.setenv("COLLECTION_HANDLE", "123.4/5678")
    monkeypatch.setenv("LOG_SOURCE_EMAIL", "noreply@example.com")
    monkeypatch.setenv("LOG_RECIPIENT_EMAIL", "mock@mock.mock")
    monkeypatch.setenv("RETRY_THRESHOLD", "10")
    monkeypatch.setenv("SENTRY_DSN", "None")


@pytest.fixture
def config_instance() -> Config:
    return Config()


@pytest.fixture
def test_aws_user():
    with mock_aws():
        user_name = "test-user"
        policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket", "sqs:GetQueueUrl"],
                    "Resource": "*",
                },
                {
                    "Effect": "Deny",
                    "Action": [
                        "dynamodb:PutItem",
                        "dynamodb:Scan",
                        "s3:GetObject",
                        "ses:SendRawEmail",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage",
                    ],
                    "Resource": "*",
                },
            ],
        }
        client = boto3.client("iam", region_name="us-east-1")
        client.create_user(UserName=user_name)
        client.put_user_policy(
            UserName=user_name,
            PolicyName="policy1",
            PolicyDocument=json.dumps(policy_document),
        )
        yield client.create_access_key(UserName="test-user")["AccessKey"]


@pytest.fixture
def sample_article():
    return Article(
        doi="10.1002/term.3131",
        metadata_url="http://example.com/works/",
        content_url="http://example.com/doi/",
        s3_client=s3_client,
        bucket="awd",
        sqs_client=sqs_client,
        sqs_base_url="https://queue.amazonaws.com/123456789012/",
        sqs_input_queue="mock-input-queue",
        sqs_output_queue="mock-output-queue",
        collection_handle="123.4/5678",
    )


@pytest.fixture
@freeze_time("2023-08-21")
def sample_doiprocessattempt(mocked_dynamodb):
    return DoiProcessAttempt(
        process_attempts=0,
        doi="10.1002/term.3131",
        last_modified=datetime.datetime.now(tz=datetime.UTC).strftime(config.DATE_FORMAT),
        status_code=1,
    )


@pytest.fixture
def s3_client():
    return S3Client()


@pytest.fixture
def ses_client():
    return SESClient(region=config.AWS_REGION_NAME)


@pytest.fixture
def sqs_client():
    return SQSClient(
        region=config.AWS_REGION_NAME,
        base_url="https://queue.amazonaws.com/123456789012/",
        queue_name="mock-output-queue",
    )


@pytest.fixture
def mocked_dynamodb():
    with mock_aws():
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            BillingMode="PAY_PER_REQUEST",
            TableName="wiley-test",
            KeySchema=[
                {"AttributeName": "doi", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "doi", "AttributeType": "S"},
            ],
        )
        DoiProcessAttempt.set_table_name("wiley-test")
        yield dynamodb


@pytest.fixture
def mocked_invalid_dynamodb():
    with mock_aws():
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            BillingMode="PAY_PER_REQUEST",
            TableName="not-a-table",
            KeySchema=[
                {"AttributeName": "doi", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "doi", "AttributeType": "S"},
            ],
        )
        yield dynamodb


@pytest.fixture
def mocked_s3():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="awd")
        yield s3


@pytest.fixture
def mocked_ses():
    with mock_aws():
        ses = boto3.client("ses", region_name="us-east-1")
        ses.verify_email_identity(EmailAddress="noreply@example.com")
        yield ses


@pytest.fixture
def mocked_sqs_input(
    sqs_client, result_message_attributes_error, result_message_body_error
):
    with mock_aws():
        sqs = boto3.resource("sqs", region_name="us-east-1")
        sqs.create_queue(QueueName="mock-input-queue")
        yield sqs


@pytest.fixture
def mocked_sqs_output():
    with mock_aws():
        sqs = boto3.resource("sqs", region_name="us-east-1")
        sqs.create_queue(QueueName="mock-output-queue")
        yield sqs


@pytest.fixture
def mocked_web(crossref_work_record_full, wiley_pdf):
    with requests_mock.Mocker() as m:
        request_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
        }
        m.get(
            "http://example.com/doi/10.1002/term.3131",
            text="Forbidden",
            status_code=403,
        )
        m.get(
            "http://example.com/doi/10.1002/term.3131",
            content=wiley_pdf,
            headers={"Content-Type": "application/pdf; charset=UTF-8"},
            request_headers=request_headers,
        )
        m.get(
            "http://example.com/doi/10.1002/none.0000",
            headers={"Content-Type": "application/html; charset=UTF-8"},
            request_headers=request_headers,
        )
        m.get(
            "http://example.com/works/10.1002/term.3131?mailto=dspace-lib@mit.edu",
            json=crossref_work_record_full,
        )
        m.get(
            "http://example.com/works/10.1002/none.0000?mailto=dspace-lib@mit.edu",
            json=crossref_work_record_full,
        )
        m.get(
            "http://example.com/works/10.1002/nome.tadata?mailto=dspace-lib@mit.edu",
            json={"data": "string"},
        )
        yield m


@pytest.fixture
def crossref_work_record_full():
    with open("tests/fixtures/crossref_work_record_full.json") as work_record:
        return json.loads(work_record.read())


@pytest.fixture
def crossref_work_record_minimum():
    with open("tests/fixtures/crossref_work_record_minimum.json") as work_record:
        return json.loads(work_record.read())


@pytest.fixture
def doi_list_insufficient_metadata():
    with open("tests/fixtures/doi_insufficient_metadata.csv", "rb") as doi_list:
        return doi_list.read()


@pytest.fixture
def doi_list_pdf_unavailable():
    with open("tests/fixtures/doi_pdf_unavailable.csv", "rb") as doi_list:
        return doi_list.read()


@pytest.fixture
def doi_list_success():
    with open("tests/fixtures/doi_success.csv", "rb") as doi_list:
        return doi_list.read()


@pytest.fixture
def wiley_pdf():
    with open("tests/fixtures/wiley.pdf", "rb") as pdf:
        return pdf.read()


@pytest.fixture
def dspace_metadata():
    with open("tests/fixtures/dspace_metadata.json") as metadata:
        return json.loads(metadata.read())


@pytest.fixture
def result_message_attributes_error():
    return {
        "PackageID": {"DataType": "String", "StringValue": "222.2/2222"},
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
    }


@pytest.fixture
def result_message_attributes_success():
    return {
        "PackageID": {"DataType": "String", "StringValue": "10.1002/term.3131"},
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
    }


@pytest.fixture
def result_message_body_error():
    return json.dumps(
        {
            "ResultType": "error",
            "ErrorTimestamp": "Thu Sep 09 18:32:39 UTC 2021",
            "ErrorInfo": "Error occurred while posting item to DSpace",
            "ExceptionMessage": "500 Server Error: Internal Server Error",
            "ExceptionTraceback": "Full unformatted stack trace of the Exception",
        }
    )


@pytest.fixture
def result_message_body_success():
    return json.dumps(
        {
            "ResultType": "success",
            "ItemHandle": "1721.1/131022",
            "lastModified": "Thu Sep 09 17:56:39 UTC 2021",
            "Bitstreams": [
                {
                    "BitstreamName": "10.1002-term.3131.pdf",
                    "BitstreamUUID": "a1b2c3d4e5",
                    "BitstreamChecksum": {
                        "value": "a4e0f4930dfaff904fa3c6c85b0b8ecc",
                        "checkSumAlgorithm": "MD5",
                    },
                }
            ],
        }
    )


@pytest.fixture
def valid_result_message(result_message_attributes_success, result_message_body_success):
    return {
        "ReceiptHandle": "lvpqxcxlmyaowrhbvxadosldaghhidsdralddmejhdrnrfeyfuphzs",
        "Body": result_message_body_success,
        "MessageAttributes": result_message_attributes_success,
    }


@pytest.fixture
def submission_message_attributes():
    return {
        "PackageID": {"DataType": "String", "StringValue": "123"},
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
        "OutputQueue": {"DataType": "String", "StringValue": "DSS queue"},
    }


@pytest.fixture
def submission_message_body():
    return json.dumps(
        {
            "SubmissionSystem": "DSpace@MIT",
            "CollectionHandle": "123.4/5678",
            "MetadataLocation": "s3://awd/10.1002-term.3131.json",
            "Files": [
                {
                    "BitstreamName": "10.1002-term.3131.pdf",
                    "FileLocation": "s3://awd/10.1002-term.3131.pdf",
                    "BitstreamDescription": None,
                }
            ],
        }
    )
