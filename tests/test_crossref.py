import json

import pytest
import requests_mock

from awd import crossref


@pytest.fixture()
def crossref_mock(work_record):
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/works/10.1002/term.3131?mailto=dspace-lib@mit.edu",
            json=work_record,
        )
        yield m


@pytest.fixture()
def work_record():
    return json.loads(open("fixtures/crossref.json", "r").read())


def test_get_dois_from_spreadsheet():
    dois = crossref.get_dois_from_spreadsheet("fixtures/test.xlsx")
    for doi in dois:
        assert doi == "10.1002/term.3131"


def test_get_crossref_work_from_dois(crossref_mock):
    doi = "10.1002/term.3131"
    work = crossref.get_crossref_work_from_doi("http://example.com/works/", doi)
    assert work["message"]["title"] == [
        "Metal‐based nanoparticles for bone tissue engineering"
    ]


def test_get_metadata_dict_from_crossref_work(crossref_mock, work_record):
    value_dict = crossref.get_metadata_dict_from_crossref_work(work_record)
    assert (
        value_dict["title"]
        == "Metal‐based nanoparticles for bone tissue engineering"
    )
    assert value_dict["publisher"] == "Wiley"
    assert value_dict["author"] == [
        "Eivazzadeh‐Keihan, Reza",
        "Bahojb Noruzi, Ehsan",
        "Khanmohammadi Chenab, Karim",
        "Jafari, Amir",
        "Radinekiyan, Fateme",
        "Hashemi, Seyed Masoud",
        "Ahmadpour, Farnoush",
        "Behboudi, Ali",
        "Mosafer, Jafar",
        "Mokhtarzadeh, Ahad",
        "Maleki, Ali",
        "Hamblin, Michael R.",
    ]
    assert value_dict["URL"] == "http://dx.doi.org/10.1002/term.3131"
    assert value_dict["container-title"] == [
        "Journal of Tissue Engineering and Regenerative Medicine"
    ]
    assert value_dict["issued"] == "2020-09-30"