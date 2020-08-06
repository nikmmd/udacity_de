# Summary

In this project I use Apache Airflow to introduce more automation and monitoring to the data warehouse ETL pipelines.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

# DAG

`etl_dag` - Sparkify ETL logic

# Helper Operators

`StageToRedshiftOperator` - Operator to `COPY` data from S3 to Redshift

`LoadFactOperator` - Operator to Load fact tables

`LoadDimensionOperator` - Operator to Load dimension tables

`DataQualityOperator` - Operator used to Validate Redshift Query results against expectation using python eval assertion.


# How to run?

1) For local run do `docker-compose up` 

This will bring up docker container with airflow and applications mounts 

2) Configure Connections

AWS:

```
Conn Id: aws_credentials

Conn Type: Enter Amazon Web Services.

Login: Enter your Access key ID from the IAM User 
credentials you downloaded earlier.

Password: Enter your Secret access key from the IAM User credentials
```

Redshift:

```
Conn Id: Enter redshift.

Conn Type: Enter Postgres.

Host: Enter the endpoint of your Redshift cluster, excluding the port at the end. You can find this by selecting your cluster in the Clusters page of the Amazon Redshift console. See where this is located in the screenshot below. IMPORTANT: Make sure to NOT include the port at the end of the Redshift endpoint string.

Schema: Enter dev. This is the Redshift database you want to connect to.

Login: Enter awsuser.

Password: Enter the password you created when launching your Redshift cluster.

Port: Enter 5439.
```
