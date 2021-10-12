import json
import os

import boto3
import pytest
import requests_mock
from moto import mock_s3


@pytest.fixture(scope="function")
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="function")
def mocked_s3(aws_credentials):
    with mock_s3():
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="awd")
        yield s3


@pytest.fixture()
def web_mock(crossref_work_record):
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/works/10.1002/term.3131?mailto=dspace-lib@mit.edu",
            json=crossref_work_record,
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
