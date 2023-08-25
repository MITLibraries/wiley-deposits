from unittest.mock import Mock

import pytest
from requests import Response

from awd.article import (
    InvalidArticleContentResponseError,
    InvalidCrossrefMetadataError,
    InvalidDSpaceMetadataError,
)


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
    dspace_metadata = {"metadata": [{"key": "dc.title", "value": "123"}]}
    assert sample_article.valid_dspace_metadata(dspace_metadata) is True


def test_valid_dspace_metadata_no_metadata(sample_article):
    dspace_metadata = {}
    assert sample_article.valid_dspace_metadata(dspace_metadata) is False


def test_valid_dspace_metadata_incorrect_fields(sample_article):
    dspace_metadata = {"metadata": [{"key": "dc.example", "value": "123"}]}
    assert sample_article.valid_dspace_metadata(dspace_metadata) is False


def test_valid_article_content_response_failure(sample_article):
    wiley_response = Response()
    wiley_response.headers = {"content-type": "application/html; charset=UTF-8"}
    assert sample_article.valid_article_content_response(wiley_response) is False


def test_valid_article_content_response_success(sample_article):
    wiley_response = Response()
    wiley_response.headers = {"content-type": "application/pdf; charset=UTF-8"}
    assert sample_article.valid_article_content_response(wiley_response) is True


def test_get_and_validate_crossref_metadata_success(
    mocked_web, sample_article, crossref_work_record_full
):
    sample_article.get_and_validate_crossref_metadata()
    assert sample_article.crossref_metadata == crossref_work_record_full


def test_get_and_validate_crossref_metadata_invalid_metadata_raises_error(
    mocked_web, sample_article
):
    sample_article.doi = "10.1002/nome.tadata"
    with pytest.raises(InvalidCrossrefMetadataError):
        sample_article.get_and_validate_crossref_metadata()


def test_create_and_validate_dspace_metadata_success(
    sample_article, crossref_work_record_full, dspace_metadata
):
    sample_article.crossref_metadata = crossref_work_record_full
    sample_article.create_and_validate_dspace_metadata()
    assert sample_article.dspace_metadata == dspace_metadata


def test_create_and_validate_dspace_metadata_invalid_metadata_raises_error(
    sample_article,
):
    sample_article.crossref_metadata = {"message": {}}
    with pytest.raises(InvalidDSpaceMetadataError):
        sample_article.create_and_validate_dspace_metadata()


def test_get_and_validate_wiley_article_content_success(
    mocked_web, sample_article, wiley_pdf
):
    sample_article.get_and_validate_wiley_article_content()
    assert sample_article.article_content == wiley_pdf


def test_get_and_validate_wiley_article_content_invalid_content_raises_error(
    mocked_web,
    sample_article,
):
    sample_article.doi = "10.1002/none.0000"
    with pytest.raises(InvalidArticleContentResponseError):
        sample_article.get_and_validate_wiley_article_content()
