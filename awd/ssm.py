from boto3 import client


class SSM:
    """An SSM class that provides a generic boto3 SSM client with specific SSM
    functionality necessary for automated Wiley deposits."""

    def __init__(self):
        self.client = client("ssm", region_name="us-east-1")

    def get_parameter_value(self, parameter_key):
        """Get parameter value based on the specified key."""
        parameter_object = self.client.get_parameter(
            Name=parameter_key, WithDecryption=True
        )
        parameter_value = parameter_object["Parameter"]["Value"]
        return parameter_value
