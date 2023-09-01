import json
import logging
from typing import Any

from pynamodb.exceptions import DoesNotExist
from requests import Response

from awd.database import DoiProcessAttempt
from awd.helpers import (
    S3Client,
    SQSClient,
    get_crossref_response_from_doi,
    get_wiley_response,
)
from awd.status import Status

logger = logging.getLogger(__name__)


class Article:
    """Article class.

    Class represents articles published by Wiley for which we will attempt to get
    metadata and binary content for uploading to our DSpace repository.
    """

    def __init__(
        self,
        doi: str,
        metadata_url: str,
        content_url: str,
        s3_client: S3Client,
        bucket: str,
        sqs_client: SQSClient,
        sqs_base_url: str,
        sqs_input_queue: str,
        sqs_output_queue: str,
        collection_handle: str,
    ) -> None:
        """Initialize article instance.

        Args:
            doi: A digital object identifer (DOI) for an article.
            metadata_url: The URL for retrieving metadata records.
            content_url: The URL for retrieving article content.
            s3_client: A configured S3 client.
            bucket: The S3 bucket for uploading metadata and article content.
            sqs_client: A configured SQS client.
            sqs_base_url: The SQS base URL to use. Enables easier unit testing.
            sqs_input_queue: The SQS input queue to use.
            sqs_output_queue: The SQS output queue to use.
            collection_handle: The handle of the DSpace collection to which items
            will be uploaded.
        """
        self.doi: str = doi
        self.metadata_url: str = metadata_url
        self.content_url: str = content_url
        self.s3_client: S3Client = s3_client
        self.bucket: str = bucket
        self.sqs_client: SQSClient = sqs_client
        self.sqs_base_url: str = sqs_base_url
        self.sqs_input_queue: str = sqs_input_queue
        self.sqs_output_queue: str = sqs_output_queue
        self.collection_handle: str = collection_handle
        self.doi_process_attempt: DoiProcessAttempt
        self.crossref_metadata: dict[str, Any]
        self.dspace_metadata: dict[str, Any]
        self.article_content: bytes

    def process(self) -> None:
        """Run the complete article processing workflow."""
        self.check_doi_and_add_to_table()
        self.get_and_validate_crossref_metadata()
        self.create_and_validate_dspace_metadata()
        self.get_and_validate_wiley_article_content()
        self.upload_files_and_send_sqs_message()

    def check_doi_and_add_to_table(self) -> None:
        """Check if DOI should be added to table.

        If not present, add the DOI to the table.  Check for unprocessed status
        and raise exception if DOI should not be retried. Increment attempts field.

        Args:
            doi: The DOI to be checked and possibly added to the DOI table.
        """
        try:
            DoiProcessAttempt.get(self.doi)
        except DoesNotExist:
            DoiProcessAttempt.add_item(self.doi)
        self.doi_process_attempt = DoiProcessAttempt.get(self.doi)
        if not self.doi_process_attempt.has_unprocessed_status():
            raise UnprocessedStatusFalseError
        self.doi_process_attempt.increment_attempts()

    def valid_crossref_metadata(self, crossref_response: Response) -> bool:
        """Validate that a Crossref work record contains sufficient metadata.

        Args:
            crossref_response: A response from Crossref to be validated.
        """
        valid = False
        if work_record := crossref_response.json():
            if (
                work_record.get("message", {}).get("title") is not None
                and work_record.get("message", {}).get("URL") is not None
            ):
                valid = True
                logger.debug("Sufficient metadata downloaded for %s", self.doi)
            else:
                logger.exception(
                    "Insufficient metadata for %s, missing title or URL", self.doi
                )
        else:
            logger.exception("Unable to parse %s response as JSON", self.doi)
        return valid

    def get_and_validate_crossref_metadata(self) -> None:
        """Get and validate metadata from Crossref API."""
        crossref_response = get_crossref_response_from_doi(self.metadata_url, self.doi)
        if self.valid_crossref_metadata(crossref_response) is False:
            raise InvalidCrossrefMetadataError
        self.crossref_metadata = crossref_response.json()

    def create_dspace_metadata(self, metadata_mapping_path: str) -> dict[str, Any]:
        """Create DSpace metadata from Crossref metadata and a metadata mapping file.

        Args:
            metadata_mapping_path: Path to a JSON metadata mapping file
        """
        keys_for_dspace = [
            "author",
            "container-title",
            "ISSN",
            "issue",
            "issued",
            "language",
            "original-title",
            "publisher",
            "short-title",
            "subtitle",
            "title",
            "URL",
            "volume",
        ]
        with open(metadata_mapping_path) as metadata_mapping_file:
            metadata_mapping = json.load(metadata_mapping_file)
            work = self.crossref_metadata["message"]
            metadata = []
            for key in [k for k in work if k in keys_for_dspace]:
                if key == "author":
                    metadata.extend(
                        [
                            {
                                "key": metadata_mapping[key],
                                "value": f'{author.get("family")}, {author.get("given")}',
                            }
                            for author in work["author"]
                        ]
                    )
                elif key == "title":
                    metadata.append(
                        {
                            "key": metadata_mapping[key],
                            "value": ". ".join(t for t in work[key]),
                        }
                    )
                elif key == "issued":
                    metadata.append(
                        {
                            "key": metadata_mapping[key],
                            "value": "-".join(
                                str(d).zfill(2) for d in work["issued"]["date-parts"][0]
                            ),
                        }
                    )
                elif isinstance(work[key], list):
                    metadata.extend(
                        [
                            {"key": metadata_mapping[key], "value": list_item}
                            for list_item in work[key]
                        ]
                    )
                else:
                    metadata.append({"key": metadata_mapping[key], "value": work[key]})
            return {"metadata": metadata}

    def valid_dspace_metadata(self, dspace_metadata: dict[str, Any]) -> bool:
        """Validate that DSpace metadata follows the expected format.

        Args:
            dspace_metadata: DSpace metadata to be validated.
        """
        approved_metadata_fields = [
            "dc.contributor.author",
            "dc.relation.journal",
            "dc.identifier.issn",
            "mit.journal.issue",
            "dc.date.issued",
            "dc.language",
            "dc.title.alternative",
            "dc.publisher",
            "dc.title",
            "dc.relation.isversionof",
            "mit.journal.volume",
        ]
        valid = False
        if dspace_metadata.get("metadata") is not None:
            for element in dspace_metadata["metadata"]:
                if (
                    element.get("key") is not None
                    and element.get("value") is not None
                    and element.get("key") in approved_metadata_fields
                ):
                    valid = True
            logger.debug("Valid DSpace metadata created")
        else:
            logger.exception("Invalid DSpace metadata created: %s ", dspace_metadata)
        return valid

    def create_and_validate_dspace_metadata(self) -> None:
        """Create and validate DSpace metadata from Crossref metadata."""
        dspace_metadata = self.create_dspace_metadata(
            "config/metadata_mapping.json",
        )
        if self.valid_dspace_metadata(dspace_metadata) is False:
            raise InvalidDSpaceMetadataError
        self.dspace_metadata = dspace_metadata

    def valid_article_content_response(self, wiley_response: Response) -> bool:
        """Validate the Wiley response contained a PDF.

        Args:
           wiley_response: A response from the Wiley server to be validated.
        """
        valid = False
        if wiley_response.headers["content-type"] == "application/pdf; charset=UTF-8":
            valid = True
            logger.debug("PDF downloaded for %s", self.doi)
        else:
            logger.exception("A PDF could not be retrieved for DOI: %s", self.doi)
            logger.debug(
                "Response contents retrieved from Wiley server for %s: %s",
                self.doi,
                wiley_response.content,
            )
        return valid

    def get_and_validate_wiley_article_content(self) -> None:
        """Get and validate article content from the Wiley server."""
        wiley_response = get_wiley_response(self.content_url, self.doi)
        if self.valid_article_content_response(wiley_response) is False:
            raise InvalidArticleContentResponseError
        self.article_content = wiley_response.content

    def upload_files_and_send_sqs_message(
        self,
    ) -> None:
        """Upload files to S3 bucket and send SQS message with the resulting S3 URIs."""
        doi_file_name = self.doi.replace("/", "-")  # 10.12/term.3131 to 10.12-term.3131

        self.s3_client.put_file(
            file_content=json.dumps(self.dspace_metadata),
            bucket=self.bucket,
            key=f"{doi_file_name}.json",
        )
        self.s3_client.put_file(
            file_content=self.article_content,
            bucket=self.bucket,
            key=f"{doi_file_name}.pdf",
        )

        s3_uri_prefix = f"s3://{self.bucket}/{doi_file_name}"

        dss_message_attributes = self.sqs_client.create_dss_message_attributes(
            package_id=self.doi,
            submission_source="wiley",
            output_queue=self.sqs_output_queue,
        )
        dss_message_body = self.sqs_client.create_dss_message_body(
            submission_system="DSpace@MIT",
            collection_handle=self.collection_handle,
            metadata_s3_uri=f"{s3_uri_prefix}.json",
            bitstream_file_name=f"{doi_file_name}.pdf",
            bitstream_s3_uri=f"{s3_uri_prefix}.pdf",
        )

        self.sqs_client.send(
            message_attributes=dss_message_attributes,
            message_body=dss_message_body,
        )

        self.doi_process_attempt.update_status(status_code=Status.MESSAGE_SENT.value)


class InvalidCrossrefMetadataError(Exception):
    pass


class InvalidDSpaceMetadataError(Exception):
    pass


class InvalidArticleContentResponseError(Exception):
    pass


class UnprocessedStatusFalseError(Exception):
    pass
