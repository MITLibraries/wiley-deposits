from boto3 import client


class SSM:
    """An SSM class that provides a generic boto3 SSM client with specific SSM
    functionality necessary for automated Wiley deposits. SSM stands for Simple
    Systems Manager (SSM), now known as AWS Systems Manager, but the SSM acronym is
    still used by boto3. This service contains Parameter Store, which is using for
    storing values that can be retrieved via an SSM client."""

    def __init__(self):
        self.client = client("ssm", region_name="us-east-1")

    def get_parameter_value(self, parameter_key):
        """Get parameter value based on the specified key."""
        parameter_object = self.client.get_parameter(
            Name=parameter_key, WithDecryption=True
        )
        parameter_value = parameter_object["Parameter"]["Value"]
        return parameter_value
