import json
import os

import boto3
import pytest
import requests_mock
from moto import mock_s3

from awd.s3 import S3
from awd.sqs import SQS


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def s3_mock(aws_credentials):
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="awd")
        yield s3


@pytest.fixture(scope="function")
def sqs_client():
    return SQS()


@pytest.fixture(scope="function")
def s3_client():
    return S3()


@pytest.fixture()
def web_mock(crossref_work_record, wiley_pdf):
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
            json={},
        )
        yield m


@pytest.fixture()
def crossref_work_record():
    return json.loads(open("tests/fixtures/crossref_work_record.json", "r").read())


@pytest.fixture()
def crossref_value_dict():
    return json.loads(open("tests/fixtures/crossref_value_dict.json", "r").read())


@pytest.fixture()
def dspace_metadata():
    return json.loads(open("tests/fixtures/dspace_metadata.json", "r").read())


@pytest.fixture()
def dss_message_attributes_example():
    dss_message_attributes = {
        "PackageID": {"DataType": "String", "StringValue": "123"},
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
        "OutputQueue": {"DataType": "String", "StringValue": "DSS queue"},
    }
    return dss_message_attributes


@pytest.fixture()
def dss_message_body_example():
    dss_message_body = {
        "SubmissionSystem": "DSpace",
        "CollectionHandle": "123.4/5678",
        "MetadataLocation": "mock://bucket/456.json",
        "Files": [
            {
                "BitstreamName": "456.pdf",
                "FileLocation": "mock://bucket/456.pdf",
                "BitstreamDescription": None,
            }
        ],
    }
    return dss_message_body


@pytest.fixture()
def wiley_pdf():
    return open("tests/fixtures/wiley.pdf", "rb").read()