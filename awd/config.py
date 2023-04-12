import logging
import os

ENV = os.getenv("WORKSPACE")

logger = logging.getLogger(__name__)
logger.debug("Configuring awd for current env: %s", ENV)

AWS_REGION_NAME = "us-east-1"

if ENV == "test":
    LOG_LEVEL = "INFO"
    DOI_TABLE = "test_dois"
    METADATA_URL = "http://example.com/works/"
    CONTENT_URL = "http://example.com/doi/"
    BUCKET = "awd"
    SQS_BASE_URL = "https://queue.amazonaws.com/123456789012/"
    SQS_INPUT_QUEUE = "mock-input-queue"
    SQS_OUTPUT_QUEUE = "mock-output-queue"
    COLLECTION_HANDLE = "123.4/5678"
    LOG_SOURCE_EMAIL = "noreply@example.com"
    LOG_RECIPIENT_EMAIL = "mock@mock.mock"
    RETRY_THRESHOLD = "10"
    SENTRY_DSN = None
else:
    LOG_LEVEL = os.getenv("LOG_LEVEL")
    DOI_TABLE = os.getenv("DOI_TABLE")
    METADATA_URL = os.getenv("METADATA_URL")
    CONTENT_URL = os.getenv("CONTENT_URL")
    BUCKET = os.getenv("BUCKET")
    SQS_BASE_URL = os.getenv("SQS_BASE_URL")
    SQS_INPUT_QUEUE = os.getenv("SQS_INPUT_QUEUE")
    SQS_OUTPUT_QUEUE = os.getenv("SQS_OUTPUT_QUEUE")
    COLLECTION_HANDLE = os.getenv("COLLECTION_HANDLE")
    LOG_SOURCE_EMAIL = os.getenv("LOG_SOURCE_EMAIL")
    LOG_RECIPIENT_EMAIL = os.getenv("LOG_RECIPIENT_EMAIL")
    RETRY_THRESHOLD = os.getenv("RETRY_THRESHOLD")
    SENTRY_DSN = os.getenv("SENTRY_DSN")
