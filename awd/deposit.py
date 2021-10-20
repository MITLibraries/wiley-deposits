import io
import json
import logging

import PyPDF2
from botocore.exceptions import ClientError
from PyPDF2.utils import PdfReadError

from awd import crossref, wiley
from awd.s3 import S3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def deposit(doi_spreadsheet_path, metadata_url, content_url, bucket):
    dois = crossref.get_dois_from_spreadsheet(doi_spreadsheet_path)
    s3 = S3()
    for doi in dois:
        work = crossref.get_crossref_work_from_doi(metadata_url, doi)
        try:
            work["message"]["title"]
            work["message"]["URL"]
            logger.info(f"Sufficient metadata downloaded for {doi}")
        except KeyError as e:
            logger.error(f"Insufficient metadata for {doi}, missing key: {e.args[0]}")
            continue
        value_dict = crossref.get_metadata_dict_from_crossref_work(work)
        metadata = crossref.create_dspace_metadata_from_dict(
            value_dict, "config/metadata_mapping.json"
        )
        wiley_response = wiley.get_wiley_response(content_url, doi)
        try:
            PyPDF2.PdfFileReader(io.BytesIO(wiley_response.content))
            logger.info(f"PDF downloaded for {doi}")
        except PdfReadError:
            logger.error(f"A PDF could not be retrieved for DOI: {doi}")
            continue
        doi_file_name = doi.replace("/", "-")
        package_files = [
            {
                "file_name": f"{doi_file_name}.json",
                "file_content": json.dumps(metadata),
            },
            {
                "file_name": f"{doi_file_name}.pdf",
                "file_content": wiley_response.content,
            },
        ]
        try:
            for file in package_files:
                s3_response = s3.put_file(
                    file["file_content"], bucket, file["file_name"]
                )
                if s3_response["ResponseMetadata"].get("HTTPStatusCode") == 200:
                    logger.info(f"{file['file_name']} uploaded to S3")
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
