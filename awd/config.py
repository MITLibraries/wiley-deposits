import logging
import os

from awd.ssm import SSM

ENV = os.getenv("WORKSPACE")
DSS_SSM_PATH = f'{os.getenv("DSS_SSM_PATH")}'
WILEY_SSM_PATH = f'{os.getenv("WILEY_SSM_PATH")}'

logger = logging.getLogger(__name__)
logger.debug("Configuring awd for current env: %s", ENV)

AWS_REGION_NAME = "us-east-1"

if ENV == "stage" or ENV == "prod":
    ssm = SSM(AWS_REGION_NAME)
    DOI_TABLE = ssm.get_parameter_value(f"{WILEY_SSM_PATH}dynamodb_table_name")
    METADATA_URL = ssm.get_parameter_value(f"{WILEY_SSM_PATH}wiley_metadata_url")
    CONTENT_URL = ssm.get_parameter_value(f"{WILEY_SSM_PATH}wiley_content_url")
    BUCKET = ssm.get_parameter_value(f"{WILEY_SSM_PATH}wiley_submit_s3_bucket")
    SQS_BASE_URL = ssm.get_parameter_value(f"{WILEY_SSM_PATH}SQS_base_url")
    SQS_INPUT_QUEUE = ssm.get_parameter_value(f"{DSS_SSM_PATH}dss_input_queue")
    SQS_OUTPUT_QUEUE = ssm.get_parameter_value(
        f"{DSS_SSM_PATH}dss_output_queues"
    ).split(",")[1]

    COLLECTION_HANDLE = ssm.get_parameter_value(
        f"{WILEY_SSM_PATH}wiley_collection_handle"
    )
    LOG_SOURCE_EMAIL = ssm.get_parameter_value(f"{WILEY_SSM_PATH}log_source_email")
    LOG_RECIPIENT_EMAIL = ssm.get_parameter_value(
        f"{WILEY_SSM_PATH}log_recipient_email"
    )
    RETRY_THRESHOLD = ssm.get_parameter_value(f"{WILEY_SSM_PATH}retry_threshold")
    SENTRY_DSN = ssm.get_parameter_value(f"{WILEY_SSM_PATH}sentry_dsn")

elif ENV == "test":
    DOI_FILE_PATH = "tests/fixtures/doi_success.csv"
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
    DOI_FILE_PATH = os.getenv("DOI_FILE_PATH")
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
