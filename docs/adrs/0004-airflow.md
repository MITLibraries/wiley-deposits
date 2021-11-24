# 4. Use Airflow for automating Wiley workflow and retire VMs with MIT IPs

## Status

Proposed

## Context

We need a way to run Wiely deposits periodically. A cron job can be easily set up on the test/production VMs we have available for Wiley. However, these VMs may not be needed anymore.  Wiley does not require IP friendly VMs. Instead Wiley has agreed to deposit files with DOIs directly to our S3 bucket. We can therefore probably retire the VMs (and reduce our VM footprint), in favor of using Airflow which can trigger AWD scripts. 

We need to test integration with Airflow first, however, as the Airflow based set up is currently complicated and the infrastructure itself may be subject to change. It may be advisable to first get a production version of Wiley ready for regular VMs, and only then investigate and transition to the Airflow infrastructure.

## Decision

We will develop an Airflow based set up and migrate the scripts to Airflow, after delivering the first production version to the existing production VM. 

## Consequences

* Using Airflow will promote reuse (advocated by our architecture principles). If we need to develop future clients (beyond Wiley) for ingesting content into DSpace, we can easily add the functionality to Airflow directly in the form of Airflow managed scripts. Using a workflow manager such as Airflow, we can configure the Wiley workflow more easily (e.g., add different steps in the pipeline depending on certain conditions or triggers).

* Wiley scripts will need to be Dockerized, as this is currently the required approach to running code in Airflow. 

* The Wiley codebase will be dependent on the Airflow service for timely triggers to DSS. If Airflow is migrated (there is a possibility that the current suboptimal Airflow infrastructure will move to AWS-managed Airflow service), we may need to make some adjustments to Wiley. Similarly, if Airflow is retired, we will need to request cloud infrastructure.

* Code deployment options to Airflow will need to be investigated.

* The current test/stage VM will not be available for any stakeholder testing (it appears that Carl Jones originally requested the VMs, before the project had started).

