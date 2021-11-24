# 3. Use the DSpace Submission Service (DSS)

## Status

Accepted

## Context

After completing the initial engineering plan for Wiley Deposits, it was
discovered that ETD has a similar use case for depositing items into DSpace. 
DSpace submission message-oriented middleware was created (based partly on original work by Eric), taking into account
architectural and use case input from the Wiley team. We want to minimize unnecessary duplicate functionality
across applications and reuse middleware, in accordance with MIT Libraries architecture principles. Preliminary 
testing indicates that DSS is functional.

## Decision

We will use the DSpace Submission Service, instead of using the DSpace API directly.

## Consequences

* The Wiley codebase will be dependent on this service for timely and error-free ingests into DSpace.
* The Wiley project will validate metadata (and PDFs) as DSS service contract may not do validation checks.
* Features not provided by DSS (e.g., notifications) will still be implemented in AWD.
