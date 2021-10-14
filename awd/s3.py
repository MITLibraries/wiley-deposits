from boto3 import client


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
        return response
