import json

import pytest
import requests_mock

from awd import crossref


@pytest.fixture()
def crossref_mock(works_generator):
    with requests_mock.Mocker() as m:
        m.get(
            "http://example.com/works/10.1002/term.3131?mailto=dspace-lib@mit.edu",
            json=works_generator[0],
        )
        yield m


@pytest.fixture()
def works_generator():
    works = [json.loads(open("fixtures/crossref.json", "r").read())]
    return works


def test_get_crossref_works_based_on_doi_spreadsheet(crossref_mock):
    works = crossref.get_crossref_works_based_on_doi_spreadsheet(
        "http://example.com/works/", "fixtures/test.xlsx"
    )
    for work in works:
        assert work["message"]["title"] == [
            "Metal‐based nanoparticles for bone tissue engineering"
        ]


def test_get_metadata_dict_from_crossref_work(crossref_mock, works_generator):
    value_dicts = crossref.get_metadata_dict_from_crossref_work(works_generator)
    for value_dict in value_dicts:
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
