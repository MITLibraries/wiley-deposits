from email.mime.multipart import MIMEMultipart
from http import HTTPStatus


def test_ses_create_email(ses_client):
    message = ses_client.create_email(
        "Email subject",
        "<html/>",
        "attachment",
    )
    assert message["Subject"] == "Email subject"
    assert message.get_payload()[0].get_filename() == "attachment"


def test_ses_send_email(mocked_ses, ses_client):
    message = message = MIMEMultipart()
    response = ses_client.send_email(
        "noreply@example.com",
        "test@example.com",
        message,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
