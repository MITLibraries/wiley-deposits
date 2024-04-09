import io
import logging
import os
from collections.abc import Iterable
from typing import Any

import sentry_sdk

logger = logging.getLogger(__name__)

AWS_REGION_NAME = "us-east-1"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class Config:
    REQUIRED_ENV_VARS: Iterable[str] = [
        "WORKSPACE",
        "SENTRY_DSN",
        "DOI_TABLE",
        "METADATA_URL",
        "CONTENT_URL",
        "BUCKET",
        "SQS_BASE_URL",
        "SQS_INPUT_QUEUE",
        "SQS_OUTPUT_QUEUE",
        "COLLECTION_HANDLE",
        "LOG_SOURCE_EMAIL",
        "LOG_RECIPIENT_EMAIL",
        "RETRY_THRESHOLD",
    ]

    OPTIONAL_ENV_VARS: Iterable[str] = ["LOG_LEVEL"]

    def __getattr__(self, name: str) -> Any:  # noqa: ANN401
        """Provide dot notation access to configurations and env vars on this class."""
        if name in self.REQUIRED_ENV_VARS or name in self.OPTIONAL_ENV_VARS:
            return os.getenv(name)
        message = f"'{name}' not a valid configuration variable"
        raise AttributeError(message)

    def check_required_env_vars(self) -> None:
        """Method to raise exception if required env vars not set."""
        missing_vars = [var for var in self.REQUIRED_ENV_VARS if not os.getenv(var)]
        if missing_vars:
            message = f"Missing required environment variables: {', '.join(missing_vars)}"
            raise OSError(message)

    def configure_logger(self, stream: io.StringIO) -> str:
        log_level = getattr(logging, self.LOG_LEVEL) if self.LOG_LEVEL else logging.INFO
        logging.basicConfig(
            format="%(levelname)-8s %(asctime)s %(message)s",
            level=log_level,
            handlers=[logging.StreamHandler(), logging.StreamHandler(stream)],
        )
        return f"Logger 'root' configured with level={logging.getLevelName(log_level)}"

    def configure_sentry(self) -> str:
        env = self.WORKSPACE
        sentry_dsn = self.SENTRY_DSN
        if sentry_dsn and sentry_dsn.lower() != "none":
            sentry_sdk.init(sentry_dsn, environment=env)
            return f"Sentry DSN found, exceptions will be sent to Sentry with env={env}"
        return "No Sentry DSN found, exceptions will not be sent to Sentry"
