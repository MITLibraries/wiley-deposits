import json
import logging

from botocore.exceptions import ClientError

from awd import crossref, s3, sqs, wiley

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def deposit(
    doi_spreadsheet_path,
    metadata_url,
    content_url,
    bucket,
    sqs_input_url,
    sqs_output_url,
    collection_handle,
):
    s3_client = s3.S3()
    sqs_client = sqs.SQS()
    dois = crossref.get_dois_from_spreadsheet(doi_spreadsheet_path)
    for doi in dois:
        crossref_work_record = crossref.get_work_record_from_doi(metadata_url, doi)
        if crossref.is_valid_response(doi, crossref_work_record) is False:
            continue
        value_dict = crossref.get_metadata_extract_from(crossref_work_record)
        metadata = crossref.create_dspace_metadata_from_dict(
            value_dict, "config/metadata_mapping.json"
        )
        wiley_response = wiley.get_wiley_response(content_url, doi)
        if wiley.is_valid_response(doi, wiley_response) is False:
            continue
        doi_file_name = doi.replace("/", "-")  # 10.1002/term.3131 to 10.1002-term.3131
        files_dict = s3.create_files_dict(
            doi_file_name, json.dumps(metadata), wiley_response.content
        )
        try:
            for file in files_dict:
                s3_client.put_file(file["file_content"], bucket, file["file_name"])
        except ClientError as e:
            logger.error(
                f"Upload failed: {file['file_name']}, {e.response['Error']['Message']}"
            )
            continue
        bitstream_s3_uri = f"s3://{bucket}/{doi_file_name}.pdf"
        metadata_s3_uri = f"s3://{bucket}/{doi_file_name}.json"
        dss_message_attributes = sqs.create_dss_message_attributes(
            doi_file_name, "wiley", sqs_output_url
        )
        dss_message_body = sqs.create_dss_message_body(
            "DSpace",
            collection_handle,
            metadata_s3_uri,
            f"{doi_file_name}.pdf",
            bitstream_s3_uri,
        )
        sqs_client.send(sqs_input_url, dss_message_attributes, dss_message_body)
    return "Submission process has completed"


if __name__ == "__main__":
    deposit()
