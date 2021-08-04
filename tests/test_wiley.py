from awd import wiley


def test_get_crossref_work_from_dois(web_mock):
    doi = "10.1002/term.3131"
    pdf = wiley.get_wiley_pdf("http://example.com/doi/", doi)
    assert pdf == b"Test content"
