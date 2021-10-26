import json
import logging

from botocore.exceptions import ClientError

from awd import crossref, s3, wiley
from awd.s3 import S3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def deposit(doi_spreadsheet_path, metadata_url, content_url, bucket):
    s3_client = S3()
    dois = crossref.get_dois_from_spreadsheet(doi_spreadsheet_path)
    for doi in dois:
        crossref_work_record = crossref.get_work_record_from_doi(metadata_url, doi)
        if crossref.is_valid_response(doi, crossref_work_record) is False:
            continue
        value_dict = crossref.get_metadata_dict_from(crossref_work_record)
        metadata = crossref.create_dspace_metadata_from_dict(
            value_dict, "config/metadata_mapping.json"
        )
        wiley_response = wiley.get_wiley_response(content_url, doi)
        if wiley.is_valid_response(doi, wiley_response) is False:
            continue
        doi_file_name = doi.replace("/", "-")  # 10.1002/term.3131 to 10.1002-term.3131
        package_files = s3.create_files_dict(
            doi_file_name, json.dumps(metadata), wiley_response.content
        )
        try:
            for file in package_files:
                s3_client.put_file(file["file_content"], bucket, file["file_name"])
        except ClientError as e:
            logger.error(
                f"Upload failed: {file['file_name']}, {e.response['Error']['Message']}"
            )
            continue
        # create dss message
        # submit dss message
    return "Submission process has completed"


if __name__ == "__main__":
    deposit()
