# 5. Storing processed DOIs to prevent duplicate items

## Status

Proposed

## Context

Given that Wiley will not provide a monthly list of only new DOIs and will instead just update a master list of DOIs, we need a system to ensure that we do not deposit the same article twice.

## Decision

We will create a list of processed DOIs that the automated workflow will check against while processing the updated master list each month. The steps of the process are outlined below:

* A .txt file with processed DOIs will be created in the S3 bucket used by this application. It will be formatted exactly like the master DOI list provided by Wiley (one DOI per line with no delimiters) so it can be processed with the same methods.
* When the automated workflow runs, it will load the processed DOIs from the .txt file into memory. It then begins iterating through the DOIs in the master list and compares each DOI to the list in memory. If it is a match with a processed DOI, the workflow skips that DOI and moves on to the next. After a DOI is processed, it is added to the processed DOI list in memory.
* After all of the DOIs have been iterated through, the processed DOI list in memory is used to overwrite the .txt file in the S3 bucket so that file is updated for the next time the workflow is run.


## Consequences

* This is a fragile solution that is vulnerable to the processed DOI file being deleted.
* A decision needs to be made regarding the stage of the workflow where a DOI should be considered processed and added to the .txt file.

