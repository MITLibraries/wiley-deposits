from email.mime.multipart import MIMEMultipart

from awd.config import STATUS_CODE_200


def test_ses_create_email(ses_class):
    message = ses_class.create_email(
        "Email subject",
        "<html/>",
        "attachment",
    )
    assert message["Subject"] == "Email subject"
    assert message.get_payload()[0].get_filename() == "attachment"


def test_ses_send_email(mocked_ses, ses_class):
    message = message = MIMEMultipart()
    response = ses_class.send_email(
        "noreply@example.com",
        "test@example.com",
        message,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == STATUS_CODE_200
