import logging
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart

from boto3 import client

logger = logging.getLogger(__name__)


class SES:
    """An SES class that provides a generic boto3 SES client."""

    def __init__(self):
        self.client = client("ses", region_name="us-east-1")

    def check_permissions(self, source_email_address, recipient_email_address):
        """Verify that an email can be sent from the specified email address"""
        self.send_email(source_email_address, recipient_email_address, MIMEMultipart())
        logger.debug(f"Email sent from: {source_email_address}")
        return (
            f"SES send from permissions confirmed for address: {source_email_address}"
        )

    def create_email(
        self,
        subject,
        attachment_content,
        attachment_name,
    ):
        """Create an email."""
        message = MIMEMultipart()
        message["Subject"] = subject
        attachment_object = MIMEApplication(attachment_content)
        attachment_object.add_header(
            "Content-Disposition", "attachment", filename=attachment_name
        )
        message.attach(attachment_object)
        return message

    def send_email(self, source_email, recipient_email_address, message):
        """Send email via SES."""
        response = self.client.send_raw_email(
            Source=source_email,
            Destinations=[recipient_email_address],
            RawMessage={
                "Data": message.as_string(),
            },
        )
        return response
