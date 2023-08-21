from unittest.mock import Mock

from awd import crossref


def test_get_dois_from_spreadsheet():
    dois = crossref.get_dois_from_spreadsheet("tests/fixtures/doi_success.csv")
    for doi in dois:
        assert doi == "10.1002/term.3131"


def test_get_crossref_work_from_doi(mocked_web):
    response = crossref.get_response_from_doi(
        "http://example.com/works/", "10.1002/term.3131"
    )
    work = response.json()
    assert work["message"]["title"] == ["Metal nanoparticles for bone tissue engineering"]


def test_is_valid_response_failure():
    response = Mock()
    response.json.return_value = {}
    validation_status = crossref.is_valid_response("111.1/111", response)
    assert validation_status is False


def test_is_valid_response_success():
    response = Mock()
    response.json.return_value = {
        "message": {"title": "Title", "URL": "http://example.com"}
    }
    validation_status = crossref.is_valid_response("111.1/111", response)
    assert validation_status is True
