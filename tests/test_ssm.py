from awd.ssm import SSM


def test_check_permissions_success(mocked_ssm):
    ssm = SSM()
    assert (
        ssm.check_permissions("/test/example/")
        == "SSM permissions confirmed for path '/test/example/'"
    )


def test_ssm_get_parameter_value(mocked_ssm):
    ssm = SSM()
    parameter_value = ssm.get_parameter_value("/test/example/collection_handle")
    assert parameter_value == "111.1/111"
