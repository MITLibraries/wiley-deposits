import json

import pytest
import requests_mock


@pytest.fixture()
def web_mock(work_record):
    with requests_mock.Mocker() as m:
        pdf = b"Test content"
        request_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36"
        }
        m.get(
            "http://example.com/doi/10.1002/term.3131",
            text="Forbidden",
            status_code=403,
        )
        m.get(
            "http://example.com/doi/10.1002/term.3131",
            content=pdf,
            request_headers=request_headers,
        )
        m.get(
            "http://example.com/works/10.1002/term.3131?mailto=dspace-lib@mit.edu",
            json=work_record,
        )
        yield m


@pytest.fixture()
def work_record():
    return json.loads(open("fixtures/crossref.json", "r").read())
