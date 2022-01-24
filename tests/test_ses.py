import os
from email.mime.multipart import MIMEMultipart

import boto3
import pytest
from botocore.exceptions import ClientError
from moto.core import set_initial_no_auth_action_count

from awd import ses


def test_check_permissions_success(mocked_ses, ses_class):
    result = ses_class.check_permissions("noreply@example.com", "mock@mock.mock")
    assert (
        result == "SES send from permissions confirmed for address: noreply@example.com"
    )


@set_initial_no_auth_action_count(0)
def test_check_permissions_raises_error_if_address_not_verified(
    mocked_ses, test_aws_user
):
    os.environ["AWS_ACCESS_KEY_ID"] = test_aws_user["AccessKeyId"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = test_aws_user["SecretAccessKey"]
    boto3.setup_default_session()
    ses_class = ses.SES()
    with pytest.raises(ClientError) as e:
        ses_class.check_permissions("noreply@example.com", "mock@mock.mock")
    assert e.value.response["Error"]["Message"] == (
        "User: arn:aws:iam::123456789012:user/test-user is not authorized to"
        " perform: ses:SendRawEmail"
    )


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
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
