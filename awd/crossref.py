import click
import pandas
import requests


def get_crossref_works_based_on_doi_spreadsheet(api_url, file):
    """Retrieve Crossref works based on a DOI from the Wiley provided spreadsheet."""
    excel_data_df = pandas.read_excel(
        file, sheet_name="MIT Article List", skiprows=range(0, 4)
    )
    for doi in excel_data_df["DOI"].tolist():
        work = requests.get(
            f"{api_url}{doi}", params={"mailto": "dspace-lib@mit.edu"}
        ).json()
        yield work


def get_metadata_dict_from_crossref_work(works):
    """Create metadata dict from a Crossref work JSON record."""
    keys_for_dspace = [
        "publisher",
        "issue",
        "page",
        "title",
        "volume",
        "author",
        "container-title",
        "original-title",
        "language",
        "subtitle",
        "short-title",
        "issued",
        "URL",
        "ISSN",
    ]
    for work in works:
        work = work["message"]
        value_dict = {}
        for key in [k for k in work.keys() if k in keys_for_dspace]:
            if key == "author":
                authors = []
                for author in work["author"]:
                    name = f'{author["family"]}, {author["given"]}'
                    authors.append(name)
                value_dict[key] = authors
            elif key == "title":
                value_dict[key] = ". ".join(t for t in work[key])
            elif key == "issued":
                issued = "-".join(
                    str(d).zfill(2) for d in work["issued"]["date-parts"][0]
                )
                value_dict[key] = issued
            else:
                value_dict[key] = work[key]
        yield value_dict


@click.command()
@click.option(
    "-d",
    "--doi_spreadsheet_path",
    required=True,
    help="The path to the DOI spreadsheet.",
)
def crossref(doi_spreadsheet_path):
    works = get_crossref_works_based_on_doi_spreadsheet(
        "https://api.crossref.org/works/", doi_spreadsheet_path
    )
    value_dicts = get_metadata_dict_from_crossref_work(works)
    for value_dict in value_dicts:
        # just printing for testing purposes
        print(value_dict)


if __name__ == "__main__":
    crossref()
