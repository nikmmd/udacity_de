# Summary 

 Mmusic streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we'll build an ETL pipeline for a database hosted on Redshift. 

This project loads data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# How to run? 

1) Create dwh.cfg: `cp dwh.example.cfg dwh.cfg` 
2) Spin up Redshift cluster by running `python cluster.py` the script will await "cluster_available" state and exit. 
3) Run ETL script: `python etl.py`
4) Optionally run analytics (`analytics.ipynb`) queries using Jupyter.

# Design 

## File desription

* `cluster.py` - Script to create multi-node Redshift cluster
* `etl.py` - ETL logic and runner
* `sql_queries` - All SQL queries 
* `analytics.ipynb` - Notebook capable of running analytical queries and contains a few example queries


## ERD



