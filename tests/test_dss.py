from awd import dss


def test_create_dss_message_attributes():
    message_attributes = dss.create_dss_message_attributes(
        "123", "Submission system", "DSS queue"
    )
    assert message_attributes == {
        "PackageID": {"DataType": "String", "StringValue": "123"},
        "SubmissionSource": {"DataType": "String", "StringValue": "Submission system"},
        "OutputQueue": {"DataType": "String", "StringValue": "DSS queue"},
    }


def test_create_dss_message_body():
    message_body = dss.create_dss_message_body(
        "DSpace",
        "123.4/5678",
        "mock://bucket/456.json",
        "456.pdf",
        "mock://bucket/456.pdf",
    )
    assert message_body == {
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
