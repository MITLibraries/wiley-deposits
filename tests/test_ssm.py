# import os

# import boto3
# import pytest
# from botocore.exceptions import ClientError
# from moto.core import set_initial_no_auth_action_count

from awd.ssm import SSM


def test_check_permissions_success(mocked_ssm):
    ssm = SSM()
    assert (
        ssm.check_permissions("/test/example/")
        == "SSM permissions confirmed for path '/test/example/'"
    )


# Unit test and comment below were copied from
# https://github.com/MITLibraries/dspace-submission-service
# Encountered similar error and commenting out test for similar reasons
#
# This test raises a weird error that seems like a moto issue. Leaving it here but
# commented out for now
# @set_initial_no_auth_action_count(0)
# def test_check_permissions_raises_error_if_no_permission(mocked_ssm, test_aws_user):
#     os.environ["AWS_ACCESS_KEY_ID"] = test_aws_user["AccessKeyId"]
#     os.environ["AWS_SECRET_ACCESS_KEY"] = test_aws_user["SecretAccessKey"]
#     # boto3.setup_default_session()
#     ssm = SSM()
#     with pytest.raises(ClientError) as e:
#         ssm.check_permissions("/test/example/")
#     assert "Access Denied" in str(e.value)


def test_ssm_get_parameter_value(mocked_ssm):
    ssm = SSM()
    parameter_value = ssm.get_parameter_value("/test/example/collection_handle")
    assert parameter_value == "111.1/111"
