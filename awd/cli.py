import click

import crossref


@click.command()
@click.option(
    "-d",
    "--doi_spreadsheet_path",
    required=True,
    help="The path to the DOI spreadsheet.",
)
def cli(doi_spreadsheet_path):
    dois = crossref.get_dois_from_spreadsheet(doi_spreadsheet_path)
    for doi in dois:
        work = crossref.get_crossref_work_from_doi(
            "https://api.crossref.org/works/", doi
        )
        value_dict = crossref.get_metadata_dict_from_crossref_work(work)
        # just printing for testing purposes
        print(value_dict)


if __name__ == "__main__":
    cli()
