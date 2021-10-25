import logging

from boto3 import client

logger = logging.getLogger(__name__)


class S3:
    """An S3 class that provides a generic boto3 s3 client for interacting with S3
    objects, along with specific S3 functionality necessary for Wiley deposits."""

    def __init__(self):
        self.client = client("s3")

    def put_file(self, file, bucket, key):
        """Put a file in a specified S3 bucket with a specified key."""
        response = self.client.put_object(
            Body=file,
            Bucket=bucket,
            Key=key,
        )
        logger.info(f"{key} uploaded to S3")
        return response


def create_package_files_dict(file_name, metadata_content, bitstream_content):
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
