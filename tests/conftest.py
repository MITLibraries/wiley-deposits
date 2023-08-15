import json
import os

import boto3
import pytest
import requests_mock
from click.testing import CliRunner
from moto import mock_dynamodb, mock_iam, mock_s3, mock_ses, mock_sqs

from awd import config
from awd.dynamodb import DynamoDB
from awd.s3 import S3
from awd.ses import SES
from awd.sqs import SQS


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture(autouse=True)
def _aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"  # noqa: S105


@pytest.fixture
def test_aws_user():
    with mock_iam():
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
def dynamodb_class():
    return DynamoDB(config.AWS_REGION_NAME)


@pytest.fixture
def s3_class():
    return S3()


@pytest.fixture
def ses_class():
    return SES(config.AWS_REGION_NAME)


@pytest.fixture
def sqs_class():
    return SQS(config.AWS_REGION_NAME)


@pytest.fixture
def mocked_dynamodb():
    with mock_dynamodb():
        dynamodb = boto3.client("dynamodb", region_name="us-east-1")
        dynamodb.create_table(
            BillingMode="PAY_PER_REQUEST",
            TableName="test_dois",
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
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="awd")
        yield s3


@pytest.fixture
def mocked_ses():
    with mock_ses():
        ses = boto3.client("ses", region_name="us-east-1")
        ses.verify_email_identity(EmailAddress="noreply@example.com")
        yield ses


@pytest.fixture
def mocked_sqs():
    with mock_sqs():
        sqs = boto3.resource("sqs", region_name="us-east-1")
        sqs.create_queue(QueueName="mock-input-queue")
        sqs.create_queue(QueueName="mock-output-queue")
        yield sqs


@pytest.fixture
def mocked_web(crossref_work_record, wiley_pdf):
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
            json=crossref_work_record,
        )
        m.get(
            "http://example.com/works/10.1002/none.0000?mailto=dspace-lib@mit.edu",
            json=crossref_work_record,
        )
        m.get(
            "http://example.com/works/10.1002/nome.tadata?mailto=dspace-lib@mit.edu",
            json={"data": "string"},
        )
        yield m


@pytest.fixture
def crossref_work_record():
    with open("tests/fixtures/crossref_work_record.json") as work_record:
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
def crossref_value_dict():
    with open("tests/fixtures/crossref_value_dict.json") as value_dict:
        return json.loads(value_dict.read())


@pytest.fixture
def wiley_pdf():
    with open("tests/fixtures/wiley.pdf", "rb") as pdf:
        return pdf.read()


@pytest.fixture
def dspace_metadata():
    with open("tests/fixtures/dspace_metadata.json") as metadata:
        return json.loads(metadata.read())


@pytest.fixture
def result_failure_message_attributes():
    return {
        "PackageID": {"DataType": "String", "StringValue": "222.2/2222"},
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
    }


@pytest.fixture
def result_success_message_attributes():
    return {
        "PackageID": {"DataType": "String", "StringValue": "111.1/1111"},
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
    }


@pytest.fixture
def result_failure_message_body():
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
def result_success_message_body():
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
