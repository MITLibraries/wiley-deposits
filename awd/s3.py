import logging
from typing import Iterator

from boto3 import client
from mypy_boto3_s3.type_defs import PutObjectOutputTypeDef

logger = logging.getLogger(__name__)


class S3:
    """An S3 class that provides a generic boto3 s3 client for interacting with S3
    objects, along with specific S3 functionality necessary for Wiley deposits."""

    def __init__(self) -> None:
        self.client = client("s3")

    def filter_files_in_bucket(
        self, bucket: str, file_type: str, excluded_prefix: str
    ) -> Iterator[str]:
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

    def archive_file_with_new_key(
        self, bucket: str, key: str, archived_key_prefix: str
    ) -> None:
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

    def put_file(
        self, file: str | bytes, bucket: str, key: str
    ) -> PutObjectOutputTypeDef:
        """Put a file in a specified S3 bucket with a specified key."""
        response = self.client.put_object(
            Body=file,
            Bucket=bucket,
            Key=key,
        )
        logger.debug(f"{key} uploaded to S3")
        return response
