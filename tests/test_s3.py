from http import HTTPStatus

import pytest
from botocore.exceptions import ClientError


def test_s3_filter_files_in_bucket_with_matching_csv(mocked_s3, s3_class):
    s3_class.put_file(
        "test1,test2,test3,test4",
        "awd",
        "test.csv",
    )
    files = s3_class.filter_files_in_bucket("awd", "csv", "archived")
    for file in files:
        assert file == "test.csv"


def test_s3_filter_files_in_bucket_without_matching_csv(mocked_s3, s3_class):
    s3_class.put_file(
        "test1,test2,test3,test4",
        "awd",
        "archived/test.csv",
    )
    files = s3_class.filter_files_in_bucket("awd", "csv", "archived")
    for file in files:
        assert file == "test.csv"


def test_archive_file_in_bucket(mocked_s3, s3_class):
    s3_class.put_file(
        "test1,test2,test3,test4",
        "awd",
        "test.csv",
    )
    s3_class.archive_file_with_new_key(
        "awd",
        "test.csv",
        "archived",
    )
    with pytest.raises(ClientError) as e:
        response = s3_class.client.get_object(Bucket="awd", Key="test.csv")
    assert (
        "An error occurred (NoSuchKey) when calling the GetObject operation: The"
        " specified key does not exist." in str(e.value)
    )
    response = s3_class.client.get_object(Bucket="awd", Key="archived/test.csv")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK


def test_s3_put_file(mocked_s3, s3_class):
    assert "Contents" not in s3_class.client.list_objects(Bucket="awd")
    s3_class.put_file(
        str({"metadata": {"key": "dc.title", "value": "A Title"}}),
        "awd",
        "test.json",
    )
    assert len(s3_class.client.list_objects(Bucket="awd")["Contents"]) == 1
    assert s3_class.client.list_objects(Bucket="awd")["Contents"][0]["Key"] == "test.json"
