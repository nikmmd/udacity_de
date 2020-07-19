# Spark data 

# Description 
In this project, I will build build an ETL pipeline for a data lake hosted on S3.

1)This project will load data from S3, 
2) process the data into analytics tables using Spark,
3) and load them back into S3.
4) I'll deploy this Spark process on a cluster using AWS.

# Instructions

1) Create new config file `cp dwh.example.cfg dhw.cfg`

2) Run with pyspark

```
pyspark --packages com.amazonaws:aws-java-sdk-pom:1.10.34,org.apache.hadoop:hadoop-aws:2.7.2 < etl.py
```

