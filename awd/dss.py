def create_dss_message_attributes(package_id, submission_source, output_queue):
    """Create attributes for a DSpace Submission Service message."""
    message_attributes = {
        "PackageID": {"DataType": "String", "StringValue": package_id},
        "SubmissionSource": {"DataType": "String", "StringValue": submission_source},
        "OutputQueue": {"DataType": "String", "StringValue": output_queue},
    }
    return message_attributes


def create_dss_message_body(
    submission_system,
    collection_handle,
    metadata_s3_uri,
    bitstream_name,
    bitstream_s3_uri,
):
    """Create body for a DSpace Submission Service message."""
    message_body = {
        "SubmissionSystem": submission_system,
        "CollectionHandle": collection_handle,
        "MetadataLocation": metadata_s3_uri,
        "Files": [
            {
                "BitstreamName": bitstream_name,
                "FileLocation": bitstream_s3_uri,
                "BitstreamDescription": None,
            }
        ],
    }
    return message_body
