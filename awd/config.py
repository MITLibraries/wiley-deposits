import logging
import os

logger = logging.getLogger(__name__)

AWS_REGION_NAME = "us-east-1"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

config_variables = [
    "WORKSPACE",
    "LOG_LEVEL",
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
    "SENTRY_DSN",
]


class Config:
    def __init__(self) -> None:
        self.load_config_variables()
        logger.info(
            "Configured wiley-deposits for env='%s'",
            self.WORKSPACE,  # type: ignore[attr-defined]
        )

    def load_config_variables(self) -> None:
        """Retrieve all required env variables and populate instance attributes."""
        for config_variable in config_variables:
            try:
                setattr(self, config_variable, os.environ[config_variable])
            except KeyError:
                logger.exception(
                    "Config error: env variable '%s' is required, please set it.",
                    config_variable,
                )
                raise
