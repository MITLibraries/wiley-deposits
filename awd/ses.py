import logging
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

from boto3 import client
from mypy_boto3_ses.type_defs import SendRawEmailResponseTypeDef

logger = logging.getLogger(__name__)


class SES:
    """An SES class that provides a generic boto3 SES client."""

    def __init__(self, region: str) -> None:
        self.client = client("ses", region_name=region)

    def create_email(
        self,
        subject: str,
        attachment_content: str,
        attachment_name: str,
    ) -> MIMEMultipart:
        """Create an email."""
        message = MIMEMultipart()
        message["Subject"] = subject
        attachment_object = MIMEApplication(attachment_content)
        attachment_object.add_header(
            "Content-Disposition", "attachment", filename=attachment_name
        )
        message.attach(attachment_object)
        return message

    def send_email(
        self, source_email: str, recipient_email_address: str, message: MIMEMultipart
    ) -> SendRawEmailResponseTypeDef:
        """Send email via SES."""
        return self.client.send_raw_email(
            Source=source_email,
            Destinations=[recipient_email_address],
            RawMessage={
                "Data": message.as_string(),
            },
        )
