from awd import wiley


def test_get_wiley_pdf(web_mock, wiley_pdf):
    doi = "10.1002/term.3131"
    response = wiley.get_wiley_response("http://example.com/doi/", doi)
    assert response.content == wiley_pdf
