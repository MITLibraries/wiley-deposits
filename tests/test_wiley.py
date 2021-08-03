import pytest
import requests_mock

from awd import wiley


@pytest.fixture()
def wiley_mock():
    with requests_mock.Mocker() as m:
        pdf = b"Test content"
        m.get(
            "http://example.com/doi/10.1002/term.3131",
            content=pdf,
        )
        yield m


def test_get_crossref_work_from_dois(wiley_mock):
    doi = "10.1002/term.3131"
    pdf = wiley.get_wiley_pdf("http://example.com/doi/", doi)
    assert pdf == b"Test content"
