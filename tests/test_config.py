import pytest

from awd.config import Config


def test_config_load_environment_variables_all_variables_present():
    config = Config()
    assert config.__dict__ == {
        "WORKSPACE": "test",
        "LOG_LEVEL": "INFO",
        "DOI_TABLE": "wiley-test",
        "METADATA_URL": "http://example.com/works/",
        "CONTENT_URL": "http://example.com/doi/",
        "BUCKET": "awd",
        "SQS_BASE_URL": "https://queue.amazonaws.com/123456789012/",
        "SQS_INPUT_QUEUE": "mock-input-queue",
        "SQS_OUTPUT_QUEUE": "mock-output-queue",
        "COLLECTION_HANDLE": "123.4/5678",
        "LOG_SOURCE_EMAIL": "noreply@example.com",
        "LOG_RECIPIENT_EMAIL": "mock@mock.mock",
        "RETRY_THRESHOLD": "10",
        "SENTRY_DSN": "None",
    }


def test_config_load_environment_variables_missing_variable_raises_keyerror(
    monkeypatch, caplog
):
    monkeypatch.delenv("LOG_LEVEL")
    with pytest.raises(KeyError):
        Config()
    assert (
        "Config error: env variable 'LOG_LEVEL' is required, please set it."
        in caplog.text
    )
