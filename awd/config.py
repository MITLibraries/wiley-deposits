import logging
import os

from awd.ssm import SSM

ENV = os.getenv("WORKSPACE")
DSS_SSM_PATH = os.getenv("DSS_SSM_PATH")
WILEY_SSM_PATH = os.getenv("WILEY_SSM_PATH")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Configuring awd for current env: %s", ENV)

AWS_REGION_NAME = "us-east-1"

if ENV == "stage" or ENV == "prod":
    ssm = SSM()
    DOI_FILE_PATH = ssm.get_parameter_value(f"{WILEY_SSM_PATH}{ENV}/doi_file_path")
    METADATA_URL = ssm.get_parameter_value(f"{WILEY_SSM_PATH}{ENV}/wiley_metadata_url")
    CONTENT_URL = ssm.get_parameter_value(f"{WILEY_SSM_PATH}{ENV}/wiley_content_url")
    BUCKET = ssm.get_parameter_value(f"{DSS_SSM_PATH}{ENV}/wiley_submit_s3_bucket")
    SQS_BASE_URL = ssm.get_parameter_value(f"{DSS_SSM_PATH}{ENV}/SQS_base_url")
    SQS_INPUT_QUEUE = ssm.get_parameter_value(
        f"{DSS_SSM_PATH}{ENV}/SQS_dss_input_queue"
    )
    SQS_OUTPUT_QUEUE = ssm.get_parameter_value(
        f"{DSS_SSM_PATH}{ENV}/SQS_dss_wiley_output_queue"
    )
    COLLECTION_HANDLE = ssm.get_parameter_value(
        f"{WILEY_SSM_PATH}{ENV}/wiley_collection_handle"
    )
    LOG_SOURCE_EMAIL = ssm.get_parameter_value(
        f"{WILEY_SSM_PATH}{ENV}/log_source_email"
    )
    LOG_RECIPIENT_EMAIL = ssm.get_parameter_value(
        f"{WILEY_SSM_PATH}{ENV}/log_recipient_email"
    )
elif ENV == "test":
    DOI_FILE_PATH = "tests/fixtures/doi_success.csv"
    METADATA_URL = "http://example.com/works/"
    CONTENT_URL = "http://example.com/doi/"
    BUCKET = "awd"
    SQS_BASE_URL = "https://queue.amazonaws.com/123456789012/"
    SQS_INPUT_QUEUE = "mock-input-queue"
    SQS_OUTPUT_QUEUE = "mock-output-queue"
    COLLECTION_HANDLE = "123.4/5678"
    LOG_SOURCE_EMAIL = "noreply@example.com"
    LOG_RECIPIENT_EMAIL = ["mock@mock.mock"]
else:
    DOI_FILE_PATH = os.getenv("DOI_FILE_PATH")
    METADATA_URL = os.getenv("METADATA_URL")
    CONTENT_URL = os.getenv("CONTENT_URL")
    BUCKET = os.getenv("BUCKET")
    SQS_BASE_URL = os.getenv("SQS_BASE_URL")
    SQS_INPUT_QUEUE = os.getenv("SQS_INPUT_QUEUE")
    SQS_OUTPUT_QUEUE = os.getenv("SQS_OUTPUT_QUEUE")
    COLLECTION_HANDLE = os.getenv("COLLECTION_HANDLE")
    LOG_SOURCE_EMAIL = os.getenv("LOG_SOURCE_EMAIL")
    LOG_RECIPIENT_EMAIL = os.getenv("LOG_RECIPIENT_EMAIL")
