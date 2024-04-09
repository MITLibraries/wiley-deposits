## The `deposit` command

### Retrieval of Unprocessed DOIs

This flowchart describes the sources of the DOIs that are processed by the Wiley `deposit` workflow.

```mermaid
---
title:
---

flowchart LR
    %% aws utilities
    dynamodb["Amazon DynamoDB"]
    s3["Amazon S3"]

    %% set of dois
    unprocessed_dois(["Unprocessed DOIs"])

    %% source graph
    dynamodb -->|"Retrieve all 'UNPROCESSED' DOIs"|unprocessed_dois
    s3 -->|"Retrieve all DOIs from CSV files"| unprocessed_dois
   
```

### Processing of DOIs

This sequence diagram depicts the processing workflow for a single DOI when the application's `deposit` command is run. 

```mermaid
---
title:
---

sequenceDiagram
    participant wiley-app as Wiley application
    participant s3 as Amazon S3
    participant dynamodb as Amazon DynamoDB
    participant ses as Amazon SES
    participant sqs as Amazon SQS
    participant crossref as Crossref
    participant wiley-online-lib as Wiley Online Library

    wiley-app ->> dynamodb: Get DOI item from table
    dynamodb -->> wiley-app: DOI item
    wiley-app ->> dynamodb: Increment (+1) process_attempts in table for DOI
    wiley-app ->> crossref: Get metadata via API request
    crossref -->> wiley-app: Crossref metadata
    wiley-app ->> wiley-app: Map Crossref metadata to DSpace metadata
    wiley-app ->> wiley-online-lib: Get PDF from Wiley via API request
    wiley-online-lib -->> wiley-app: PDF
    wiley-app ->> s3: Upload DSpace metadata JSON and Wiley PDF
    wiley-app ->> sqs: Send message to 'dss-wiley-output' queue
    wiley-app ->> wiley-app: Filter log streams to ERROR messages
    wiley-app ->> ses: Send an email to stakeholders (subject: "Automated Wiley deposit errors")
```

## The `listen` command

### Processing of DSS messages
This sequence diagram depicts the processing workflow for a single message from the [dspace-submission-service](https://github.com/MITLibraries/dspace-submission-service/tree/main) when the application's `listen` command is run.

```mermaid
---
title: 
---

sequenceDiagram
    participant wiley-app as Wiley application
    participant dynamodb as Amazon DynamoDB
    participant ses as Amazon SES
    participant sqs as Amazon SQS

    wiley-app ->> sqs: Retrieve DSS message for the DOI
    sqs -->> wiley-app: DSS message 
    wiley-app ->> dynamodb: Get DOI item from table
    dynamodb -->> wiley-app: DOI item
    wiley-app ->> wiley-app: Log result of DSpace submission for the DOI
    alt submission was successful
        wiley-app ->> dynamodb: Set DOI's status to 'SUCCESS'
    else submission result in error
        alt DOI's process_attempts >= retry threshold 
            wiley-app ->> dynamodb: Set DOI's status to 'FAILED'
        else DOI's process_attempts < retry threshold
            wiley-app ->> dynamodb: Reset DOI's status to 'UNPROCESSED'
        end
    end
    wiley-app ->> ses: Send an email to stakeholders (subject: "DSS results")


```