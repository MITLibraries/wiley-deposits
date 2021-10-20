from awd.s3 import S3


def test_s3_put_file(s3_mock):
    s3 = S3()
    assert "Contents" not in s3.client.list_objects(Bucket="awd")
    s3.put_file(
        str({"metadata": {"key": "dc.title", "value": "A Title"}}),
        "awd",
        "test.json",
    )
    assert len(s3.client.list_objects(Bucket="awd")["Contents"]) == 1
    assert s3.client.list_objects(Bucket="awd")["Contents"][0]["Key"] == "test.json"
