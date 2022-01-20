from email.mime.multipart import MIMEMultipart

import boto3
from moto import mock_ses

from awd import ses


def test_ses_create_email():
    message = ses.SES().create_email(
        "Email subject",
        "<html/>",
        "attachment",
    )
    assert message["Subject"] == "Email subject"
    assert message.get_payload()[0].get_filename() == "attachment"


@mock_ses
def test_ses_send_email():
    ses_client = boto3.client("ses", region_name="us-east-1")
    ses_client.verify_email_identity(EmailAddress="noreply@example.com")
    message = message = MIMEMultipart()
    response = ses.SES().send_email(
        "noreply@example.com",
        "test@example.com",
        message,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
