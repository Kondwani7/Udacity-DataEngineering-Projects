# Udacity Data Engineer Nanodegree
![Interface](https://napaanalytics.com/wp-content/uploads/2020/04/Napa-Data-Engineering-Image.jpg)

## Table of contents:
- [Data modelling with Postgres](#datamodelingpostgres)
- [Data modelling with cassandra](#datamodelingcassandra)
- [The primary scripts of the project](#clouddatawarehouse)
- [Data Lake](#datalake)
- [Data pipelines with Airflow](datapipelines)
- [Capstone Project](#capstoneproject)

### Summary
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## DataModelingPostgres
### objective
Build an ETL pipeline that extracts their data and inserts them into postgres tables. [Project Link](https://github.com/Kondwani7/Udacity-DataEngineering-Projects/tree/main/1A_data_modeling_postgres)

## DataModelingCassandra
### Objective
Build an ETL pipeline that extracts their data and inserts them into apache cassandra table. [Project Link](https://github.com/Kondwani7/Udacity-DataEngineering-Projects/tree/main/1b_data_modeling_cassandra)

## CloudDataWarehouse
### Objective
Build an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to. [Project Link](https://github.com/Kondwani7/Udacity-DataEngineering-Projects/tree/main/2_cloud_data_warehouse)

## DataLake
### Objective
build an ETL pipeline that extracts their data from S3, processes them using Spark (AWS Elastic Map Reduce(EMR)), and loads the data back into S3 as a set of dimensional tables. [Project Link](https://github.com/Kondwani7/Udacity-DataEngineering-Projects/tree/main/2_cloud_data_warehouse)

## DataPipelines
### Objectives
- create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills.
- Add data quality  to catch any discrepancies in the datasets.
- Process the source data in S3 to  Amazon Redshift. 
- [Project Link](https://github.com/Kondwani7/Udacity-DataEngineering-Projects/tree/main/4_data_pipelines)

## CapstoneProject
### Objective
Transform Amazon bestsellers and reviews data to predict how reviews determine a sellers store performance. [Project Link](https://github.com/Kondwani7/Udacity-DataEngineering-Projects/tree/main/5_capstone_project)
