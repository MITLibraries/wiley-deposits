# wiley-deposits

Wiley-Deposits is a Python CLI application for processing MIT-affiliated author manuscripts from Wiley. It is part of a workflow that uploads Wiley publications to [DSpace@MIT](https://dspace.mit.edu/) via the [Dspace Submission Service (DSS)](https://github.com/MITLibraries/dspace-submission-service).

At a high level, this is accomplished by:

1. Running the `deposit` command to retrieve metadata and files from Wiley and submit messages to DSS.

2. Running DSS to process messages from previous step and upload metadata and bitstreams (files) to DSpace@MIT.

3. Running the `listen` command to update a DynamoDB table that tracks all Wiley deposits.

Diagrams describing the workflow in greater detail are provided in [docs/wiley_commands.md](docs/wiley_commands.md).


## Development

- To preview a list of available Makefile commands: `make help`
- To install with dev dependencies: `make install`
- To update dependencies: `make update`
- To run unit tests: `make test`
- To lint the repo: `make lint`
- To run the app: `pipenv run awd --help`

## Environment Variables

### Required

```
WORKSPACE=### Set to 'dev' for local development, this will be set to 'stage' and 'prod' in those environments by Terraform.

SENTRY_DSN=### If set to a valid Sentry DSN, enables Sentry exception monitoring. This is not needed for local development.

DOI_TABLE=### The name of the DynamoDB table tracking Wiley deposits, e.g. 'wiley-<env>'.

METADATA_URL=### URL for the Crossref REST API used to retrieve metadata, i.e., "https://api.crossref.org/works/".

CONTENT_URL=### Base URL for downloading PDFs from the Wiley Online Library.

BUCKET=### S3 bucket storing CSVs from Wiley and downloaded content for publications.

SQS_BASE_URL=### Base URL for message queuing service.

SQS_INPUT_QUEUE=### Name of the queue for DSS, e.g. 'dss-input-<env>'.

SQS_OUTPUT_QUEUE=### Name of the queue used in tracking Wiley deposits via the DynamoDB table, e.g. 'dss-wiley-output-<env>'.

COLLECTION_HANDLE=### Collection handle for the 'MIT Open Access Articles' on DSpace@MIT.

LOG_SOURCE_EMAIL=### Source email address used in the email for Wiley-Deposit errors.

LOG_RECIPIENT_EMAIL=### Recipient email address used in the email for Wiley-Deposit errors.

RETRY_THRESHOLD=### Maximum number of attempts allowed to process a Wiley publication.
```

### Optional

```
LOG_LEVEL=### Logging level. Defaults to 'INFO'.
```

## CLI Commands

### `awd`

```
Usage: -c [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  deposit  Process DOIs from .csv files and unprocessed DOIs from DynamoDB.
  listen   Retrieve messages from an SQS queue and email the results to stakeholders.
```

### `awd deposit` 

```
Usage: -c deposit [OPTIONS]

  Process DOIs from .csv files and unprocessed DOIs from DynamoDB.

  Retrieve metadata and PDFs for the DOI and send a message to an SQS queue.
  Errors generated during the process are emailed to stakeholders.

Options:
  --help  Show this message and exit.
```

### `awd listen`

```
Usage: -c listen [OPTIONS]

  Retrieve messages from an SQS queue and email the results to stakeholders.

Options:
  --help  Show this message and exit.
```
