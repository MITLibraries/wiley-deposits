import logging
import os
from collections.abc import Iterable

logger = logging.getLogger(__name__)

AWS_REGION_NAME = "us-east-1"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class Config:
    REQUIRED_ENVIRONMENT_VARIABLES: Iterable[str] = (
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
    )

    WORKSPACE: str
    LOG_LEVEL: str
    DOI_TABLE: str
    METADATA_URL: str
    CONTENT_URL: str
    BUCKET: str
    SQS_BASE_URL: str
    SQS_INPUT_QUEUE: str
    SQS_OUTPUT_QUEUE: str
    COLLECTION_HANDLE: str
    LOG_SOURCE_EMAIL: str
    LOG_RECIPIENT_EMAIL: str
    RETRY_THRESHOLD: str
    SENTRY_DSN: str

    def __init__(self) -> None:
        self.load_environment_variables()
        logger.info("Configured wiley-deposits for env='%s'", self.WORKSPACE)

    def load_environment_variables(self) -> None:
        """Retrieve required environment variables and populate instance attributes."""
        for config_variable in self.REQUIRED_ENVIRONMENT_VARIABLES:
            try:
                setattr(self, config_variable, os.environ[config_variable])
            except KeyError:
                logger.exception(
                    "Config error: env variable '%s' is required, please set it.",
                    config_variable,
                )
                raise
