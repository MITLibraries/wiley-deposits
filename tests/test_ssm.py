def test_check_permissions_success(mocked_ssm, ssm_class):
    assert (
        ssm_class.check_permissions("/test/example/")
        == "SSM permissions confirmed for path '/test/example/'"
    )


def test_ssm_get_parameter_value(mocked_ssm, ssm_class):
    parameter_value = ssm_class.get_parameter_value("/test/example/collection_handle")
    assert parameter_value == "111.1/111"
