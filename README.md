IEX workforce - Landing to Bronze ETl

Overview

  This project implements an AWS glue ETL pipelines for processing IEX workforce Agent Activity data from the S3 landing layer to bronze layer.
This pipeline supports data validation,seperation of valid and invalid records, and quarantine handling for rejected records.

AWS Services

Awazon S3: Input, Bronze output, and quarantine storage
AWS GLUE: ETL processing and data validation
AWS GLUE crawlers - Cataloging Bronze and quarantine data
AWS Glue data catalog - Table metadata
AWS IAM - Access Management

Processing Flow
1.  Read input data from the s3 Landing bucket blue-lake-landing-dev
2.  Apply configured data validation rules and file validation.
3.  Separate valid records fron rejected records.
4.  Write valid records to the bronze layer in Parquet format.
5.  Write rejected records to the quarantine loaction with rejection reason.
6.  Use glue crawlers to register output tables.
7.  Query the results using Amazon athena.

Validation and testing
The test scenarios include:

  1.  Missing mandatory fields.
  2.  Null values in mandatory fields.
  3.  Invalid Json
  4.  Empty input files.
  5.  Invalid record structures.

Verified negative test

Scenario: Null value in mandatory field acdlogon_agtoid

Observed rejection reason : Null value in acdlogon_agtoid

Result: Passed

The rejected record was verfied in the quarantine s3 location and successfully queried through amazon athena.

Execution:
  The ETL job is configured and executed in the authorized AWS developement environment.

Execution parameters, IAM permission, and source and target loactions must be configured according to the deployment environment.

Repository Structure : 

The repository contains the Glue ETL implementation and its supporting configuration and documentation. Go to the source folders for more information

Development workflow:

dev- shared development integration branch
feature/* - individual development branches
Pull requests - used to review and merge changes into dev.

Current Status :

Landing - to Bronze Glue ETL implementation completed for agent_activity feed.
Bronze and quarantine S3 output locations configured
Aws Glue crawlers created and executed successfully.
Athena validation completed and quarantined data.
Manadatory field NULL validation tested successfully.
Additional negative test scenarios and reconciliation checks completed 
