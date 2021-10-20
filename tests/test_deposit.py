import logging

from awd import deposit
from awd.s3 import S3

logger = logging.getLogger(__name__)


def test_deposit_success(web_mock, s3_mock, s3_client):
    s3 = S3()
    response = deposit.deposit(
        "tests/fixtures/doi_success.csv",
        "http://example.com/works/",
        "http://example.com/doi/",
        "awd",
    )
    uploaded_metadata = s3.client.get_object(Bucket="awd", Key="10.1002-term.3131.json")
    assert uploaded_metadata["ResponseMetadata"]["HTTPStatusCode"] == 200
    uploaded_bitstream = s3.client.get_object(Bucket="awd", Key="10.1002-term.3131.pdf")
    assert uploaded_bitstream["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response == "Submission process has completed"


def test_deposit_insufficient_metadata(caplog, web_mock, s3_mock, s3_client):
    with caplog.at_level(logging.INFO):
        response = deposit.deposit(
            "tests/fixtures/doi_insufficient_metadata.csv",
            "http://example.com/works/",
            "http://example.com/doi/",
            "awd",
        )
        assert (
            "Insufficient metadata for 10.1002/nome.tadata, missing key: message"
            in caplog.text
        )
        assert "Contents" not in s3_client.client.list_objects(Bucket="awd")
        assert response == "Submission process has completed"


def test_deposit_pdf_unavailable(caplog, web_mock, s3_mock, s3_client):
    with caplog.at_level(logging.INFO):
        response = deposit.deposit(
            "tests/fixtures/doi_pdf_unavailable.csv",
            "http://example.com/works/",
            "http://example.com/doi/",
            "awd",
        )
        assert "A PDF could not be retrieved for DOI: 10.1002/none.0000" in caplog.text
        assert "Contents" not in s3_client.client.list_objects(Bucket="awd")
        assert response == "Submission process has completed"


def test_deposit_s3_upload_failed(caplog, web_mock, s3_mock, s3_client):
    with caplog.at_level(logging.INFO):
        response = deposit.deposit(
            "tests/fixtures/doi_success.csv",
            "http://example.com/works/",
            "http://example.com/doi/",
            "not-a-bucket",
        )
    assert "Upload failed: 10.1002-term.3131.json" in caplog.text
    assert "Contents" not in s3_client.client.list_objects(Bucket="awd")
    assert response == "Submission process has completed"
