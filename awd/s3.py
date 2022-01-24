import logging

from boto3 import client

logger = logging.getLogger(__name__)


class S3:
    """An S3 class that provides a generic boto3 s3 client for interacting with S3
    objects, along with specific S3 functionality necessary for Wiley deposits."""

    def __init__(self):
        self.client = client("s3")

    def check_permissions(self, bucket):
        """Checks S3 ListObjectV2 and GetObject permissions for a bucket. If either
        command is not allowed for any of the provided buckets, raises an Access
        Denied bocotore client error.
        """
        response = self.client.list_objects_v2(Bucket=bucket, MaxKeys=1)
        logger.debug(f"Successfully listed objects in bucket '{bucket}'")
        for object in response["Contents"]:
            self.client.get_object(Bucket=bucket, Key=object["Key"])
            logger.debug(
                f"Successfully retrieved object '{object['Key']}' from bucket "
                f"'{bucket}'"
            )
        return (
            f"S3 list objects and get object permissions confirmed for bucket: {bucket}"
        )

    def filter_files_in_bucket(self, bucket, file_type, excluded_prefix):
        """Retrieve file with the specified file extension in the specified bucket without
        the excluded prefix."""
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket)
        for object in [
            object
            for page in pages
            for object in page["Contents"]
            if object["Key"].endswith(file_type)
            and excluded_prefix not in object["Key"]
        ]:
            yield object["Key"]

    def archive_file_with_new_key(self, bucket, key, archived_key_prefix):
        """Change the key of the specified file in the specified bucket to archive it from
        processing"""
        self.client.copy_object(
            Bucket=bucket,
            CopySource=f"{bucket}/{key}",
            Key=f"{archived_key_prefix}/{key}",
        )
        self.client.delete_object(
            Bucket=bucket,
            Key=key,
        )

    def put_file(self, file, bucket, key):
        """Put a file in a specified S3 bucket with a specified key."""
        response = self.client.put_object(
            Body=file,
            Bucket=bucket,
            Key=key,
        )
        logger.debug(f"{key} uploaded to S3")
        return response


def create_files_dict(file_name, metadata_content, bitstream_content):
    """Create dict of files to upload to S3."""
    package_files = [
        {
            "file_name": f"{file_name}.json",
            "file_content": metadata_content,
        },
        {
            "file_name": f"{file_name}.pdf",
            "file_content": bitstream_content,
        },
    ]
    return package_files
