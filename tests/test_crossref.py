from awd import crossref


def test_get_dois_from_spreadsheet():
    dois = crossref.get_dois_from_spreadsheet("tests/fixtures/doi_success.csv")
    for doi in dois:
        assert doi == "10.1002/term.3131"


def test_get_crossref_work_from_dois(web_mock):
    doi = "10.1002/term.3131"
    work = crossref.get_work_record_from_doi("http://example.com/works/", doi)
    assert work["message"]["title"] == [
        "Metal‐based nanoparticles for bone tissue engineering"
    ]


def test_get_metadata_extract_from(web_mock, crossref_value_dict, crossref_work_record):
    value_dict = crossref.get_metadata_extract_from(crossref_work_record)
    assert value_dict == crossref_value_dict


def test_create_dspace_metadata_from_dict_minimum_metadata():
    value_dict = {
        "title": "Metal‐based nanoparticles for bone tissue engineering",
        "URL": "http://dx.doi.org/10.1002/term.3131",
    }
    metadata = crossref.create_dspace_metadata_from_dict(
        value_dict, "config/metadata_mapping.json"
    )
    assert metadata["metadata"] == [
        {
            "key": "dc.title",
            "value": "Metal‐based nanoparticles for bone tissue engineering",
        },
        {
            "key": "dc.relation.isversionof",
            "value": "http://dx.doi.org/10.1002/term.3131",
        },
    ]


def test_transform_dict_with_metadata_mapping_full_metadata(
    crossref_value_dict, dspace_metadata
):
    metadata = crossref.create_dspace_metadata_from_dict(
        crossref_value_dict, "config/metadata_mapping.json"
    )
    assert metadata == dspace_metadata


def test_is_valid_dspace_metadata_success():
    validation_status = crossref.is_valid_dspace_metadata(
        {"metadata": [{"key": "dc.title", "value": "123"}]}
    )
    assert validation_status is True


def test_is_valid_dspace_metadata_no_metadata():
    validation_status = crossref.is_valid_dspace_metadata({})
    assert validation_status is False


def test_is_valid_dspace_metadata_incorrect_fields():
    validation_status = crossref.is_valid_dspace_metadata(
        {"metadata": [{"key": "dc.example", "value": "123"}]}
    )
    assert validation_status is False


def test_is_valid_response_failure():
    validation_status = crossref.is_valid_response("111.1/111", {})
    assert validation_status is False


def test_is_valid_response_success():
    validation_status = crossref.is_valid_response(
        "111.1/111", {"message": {"title": "Title", "URL": "http://example.com"}}
    )
    assert validation_status is True
