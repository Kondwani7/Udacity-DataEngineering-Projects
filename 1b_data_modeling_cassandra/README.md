# Data Modelling with Postgres
## Project introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

## Table of contents:

- [Objective](#objective)
- [steps needed to run project](#runproject)
- [Datasets](#datasetsource)
- [Additional Steps that can be taken](#additionalsteps)


## Objective
- Complete data modeling with Apache Cassandra
- Create an ETL pipeline using Python



### RunProject
run the notebook provided in the workspace

## DatasetSource
For this project, you'll be working with one dataset: event_data. 
The directory of CSV files partitioned by date. Here are examples of filepaths to two files in the dataset:
```bash
event_data/2018-11-08-events.csv
event_data/2018-11-09-events.csv
```
Representing data from user log events on  10th an 11th  Novmember 2018 (2 days)
## AdditionalSteps
### query 1
- session Id was chosen as the primary key while the itemInSession was the composite key to optimize our data's partition across cassandra's nodes and potentially faster queries based on the design of table session_item.
- The primary intererst was queries based on session events related to each artist
- As the data gets larger, say events data over 1 year, partitioning the data in this format may lead to faster queries

### query2
- The userid and session Id were chosen as the primary key while the itemInSession was the composite key to optimize our data's partition across cassandra's nodes and potentially faster queries based on the design of table user_session. 
- The primary interest was queries based on the user events in their session. 
- As the data gets larger, say events data over 1 year, partitioning the data in this format may lead to faster queries

### query3
- The song and userId were chosen as the primary key while the itemInSession was the composite key to optimize our data's partition across cassandra's nodes and potentially faster queries based on the design  of table song. 
- The primary interest was to optimize queries based on the song table. 
- As the data gets larger, say events data over 1 year, partitioning the data in this format may lead to faster queries

- note that certain queries such as 
```bash
SELECT  artist, song, length FROM session_item WHERE sessionId='338' AND itemInSession='4'
```
and
```bash
SELECT artist, song, itemInSession ,song, firstName, lastName FROM user_session WHERE userId='10' AND sessionId='182'
```
returned no values because sessionId=338 and sessionId=210  did not exist
