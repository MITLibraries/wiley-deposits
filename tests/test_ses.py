from email.mime.multipart import MIMEMultipart
from http import HTTPStatus


def test_ses_create_email(ses_instance):
    message = ses_instance.create_email(
        "Email subject",
        "<html/>",
        "attachment",
    )
    assert message["Subject"] == "Email subject"
    assert message.get_payload()[0].get_filename() == "attachment"


def test_ses_send_email(mocked_ses, ses_instance):
    message = message = MIMEMultipart()
    response = ses_instance.send_email(
        "noreply@example.com",
        "test@example.com",
        message,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == HTTPStatus.OK
