# 5. Storing DOI state in DynamoDB

## Status

Proposed

## Context

Given that Wiley will not provide a monthly list of only new DOIs and will instead just update a master list of DOIs, we need a system to ensure that we do not deposit the same content twice.

## Decision

We will create an AWS DynamoDB to track the status of each Wiley-provided DOI in the workflow. The steps are outlined below:

* The database will consist of a single table with rows containing information about DOIs that are processed in this workflow.
* Each row will contain the DOI, the status of the DOI in the workflow, and the number of attempts to process the DOI.
* The values for the status are:
    * Processing
    * Success
    * Failed and will retry
    * Failed after too many attempts
* When a new master list is deposited by Wiley, the workflow will iterate through the list, adding each new DOI and setting the status to `Processing`.
* Each time the workflow is run, it will attempt to process every DOI with a status of `Processing` and `Failed and will retry`. If the DSpace Submission Service returns a success message to indicate a successful deposit, the status is changed to `Success` and it will not be processed again. If the DSpace Submission Service returns an error message, the status is changed to `Failed and will retry` and it will be processed again the next time the workflow is run.
* The workflow attempts to process every DOI with the status of `Failed and will retry` until the number of attempts reaches the decided-upon threshold. Once the threshold is reached, the status of the DOI is changed to `Failed after too many attempts`.
* When a DOI reaches the status of `Failed after too many attempts`, the workflow will not process that DOI in future runs and a notifcation will be sent to stakeholders to let them know that the metadata and content for that DOI will need to be deposited manually into DSpace. 

## Consequences

* There is currently minimal data recorded for each DOI which influenced the decision to use a simple solution like DynamoDB. If the workflow changes and more data needs to be recorded, we may need to choose a more complex database solution.
* Decisions need to be made on how frequently this workflow will be run and the threshold for the number of attempts before the workflow stops trying to process a DOI.