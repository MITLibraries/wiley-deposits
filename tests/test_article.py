from unittest.mock import Mock

from requests import Response

from awd.article import Article
from awd.status import Status


def test_exists_in_database_true():
    article = Article("222.2/2222")
    database_items = [{"doi": "111.1/1111"}]
    assert article.exists_in_database(database_items) is True


def test_exists_in_database_false(sample_article):
    database_items = [{"doi": "111.1/1111"}]
    assert sample_article.exists_in_database(database_items) is False


def test_has_retry_status_true(sample_article):
    database_items = [{"doi": "111.1/1111", "status": str(Status.UNPROCESSED.value)}]
    assert sample_article.has_retry_status(database_items) is True


def test_has_retry_status_false(sample_article):
    database_items = [{"doi": "111.1/1111", "status": str(Status.SUCCESS.value)}]
    assert sample_article.has_retry_status(database_items) is False


def test_create_dspace_metadata_minimum_metadata(
    sample_article, crossref_work_record_minimum
):
    sample_article.crossref_metadata = crossref_work_record_minimum
    metadata = sample_article.create_dspace_metadata(
        "config/metadata_mapping.json",
    )
    assert metadata["metadata"] == [
        {
            "key": "dc.title",
            "value": "Metal nanoparticles for bone tissue engineering",
        },
        {
            "key": "dc.relation.isversionof",
            "value": "http://dx.doi.org/10.1002/term.3131",
        },
    ]


def test_create_dspace_metadata_full_metadata(
    sample_article, crossref_work_record_full, dspace_metadata
):
    sample_article.crossref_metadata = crossref_work_record_full
    metadata = sample_article.create_dspace_metadata("config/metadata_mapping.json")
    assert metadata == dspace_metadata


def test_valid_crossref_metadata_failure(sample_article):
    response = Mock()
    response.json.return_value = {}
    assert sample_article.valid_crossref_metadata(response) is False


def test_valid_crossref_metadata_success(sample_article):
    response = Mock()
    response.json.return_value = {
        "message": {"title": "Title", "URL": "http://example.com"}
    }
    assert sample_article.valid_crossref_metadata(response) is True


def test_valid_dspace_metadata_success(sample_article):
    sample_article.dspace_metadata = {"metadata": [{"key": "dc.title", "value": "123"}]}
    assert sample_article.valid_dspace_metadata() is True


def test_valid_dspace_metadata_no_metadata(sample_article):
    sample_article.dspace_metadata = {}
    assert sample_article.valid_dspace_metadata() is False


def test_valid_dspace_metadata_incorrect_fields(sample_article):
    sample_article.dspace_metadata = {"metadata": [{"key": "dc.example", "value": "123"}]}
    assert sample_article.valid_dspace_metadata() is False


def test_valid_article_content_response_failure(sample_article):
    wiley_response = Response()
    wiley_response.headers = {"content-type": "application/html; charset=UTF-8"}
    assert sample_article.valid_article_content_response(wiley_response) is False


def test_valid_article_content_response_success(sample_article):
    wiley_response = Response()
    wiley_response.headers = {"content-type": "application/pdf; charset=UTF-8"}
    assert sample_article.valid_article_content_response(wiley_response) is True
