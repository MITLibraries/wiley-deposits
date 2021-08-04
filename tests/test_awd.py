from awd import crossref, wiley


def test_awd(web_mock):
    dois = crossref.get_dois_from_spreadsheet("fixtures/test.xlsx")
    for doi in dois:
        work = crossref.get_crossref_work_from_doi("http://example.com/works/", doi)
        value_dict = crossref.get_metadata_dict_from_crossref_work(work)
        pdf = wiley.get_wiley_pdf("http://example.com/doi/", doi)
        # value_dict transformed into DSpace metadata
        # DSpace metadata and pdf sent to DSpace submission service
        assert pdf == b"Test content"
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
