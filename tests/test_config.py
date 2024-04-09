import io

import pytest


def test_config_env_var_access_success(config_instance):
    assert config_instance.WORKSPACE == "test"


def test_config_env_var_access_error(config_instance):
    with pytest.raises(
        AttributeError, match="'DOES_NOT_EXIST' not a valid configuration variable"
    ):
        _ = config_instance.DOES_NOT_EXIST


def test_config_check_required_env_vars_success(config_instance):
    config_instance.check_required_env_vars()


def test_config_check_required_env_vars_error(monkeypatch, config_instance):
    monkeypatch.delenv("WORKSPACE")
    with pytest.raises(OSError, match="Missing required environment variables"):
        config_instance.check_required_env_vars()


def test_config_configure_logger(config_instance):
    assert (
        config_instance.configure_logger(stream=io.StringIO())
        == "Logger 'root' configured with level=INFO"
    )
