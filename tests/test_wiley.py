import requests

from awd import wiley


def test_get_wiley_pdf(mocked_web, wiley_pdf):
    doi = "10.1002/term.3131"
    response = wiley.get_wiley_response("http://example.com/doi/", doi)
    assert response.content == wiley_pdf


def test_is_valid_response_failure():
    wiley_response = requests.Response()
    wiley_response.headers = {"content-type": "application/html; charset=UTF-8"}
    validation_status = wiley.is_valid_response("111.1/111", wiley_response)
    assert validation_status is False


def test_is_valid_response_success():
    wiley_response = requests.Response()
    wiley_response.headers = {"content-type": "application/pdf; charset=UTF-8"}
    validation_status = wiley.is_valid_response("111.1/111", wiley_response)
    assert validation_status is True
