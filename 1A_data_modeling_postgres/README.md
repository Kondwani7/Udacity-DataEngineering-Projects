# Data Modelling with Postgres
## Project introduction

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query their data, which resides in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Table of contents:

- [Objective](#objective)
- [Dimension and fact Table](#starschema)
- [The primary scripts of the project](#scripts)
- [steps needed to run project](#runproject)
- [Datasets](#datasetsource)
- [Additional Steps that can be taken](#additionalsteps)


## Objective
Build an ETL pipeline that extracts their data and inserts them into postgres tables, modeled with a star schema in mind

## StarSchema
- songplays: fact table - (songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
- songs: dimensions table - (song_id, title, artist_id, year, duration)
- artists: dimensions table - (artist_id, name, location, lattitude, longitude)
- users: dimensions table - (user_id, first_name, last_name, gender, level)
- time : dimension table - (start_time, hour, day, week, month, year, weekday)

### Scripts
the main scripts in this project are
- sql_queries.py: It contains the SQL statements needed to create and insert the data into the tables that form our star schema
- create_tables.py: It's the script that creates our tables and drops them after use to reset the database's tables
- etl.py : It's the script that extracts all the data in json format and converts them in a dataframe that can be inserted into the tables in our star schema
- test.ipynb: to run analytic queries and assert if data was added into the tables in our star schema
- etl.ipynb: primarily used to interact with the subset of the data in the pipeline and experiment with some additional queries

### RunProject
First run create_tables.py script found in the file directory
```bash
python create_tables.py
```
To test the tables created before and after the etl.py script
```bash 
test.ipynb
```
Finally, run the etl.py file in the project to extract the json files and insert them into the postgres tables  finish the pipeline
```bash
python etl.py
```
To experiment with a subset of the data used in the pipeline, review the sections in
```bash
etl.ipynb
```
## DatasetSource
### Song dataset
The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/). Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
###Log dataset
The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above. These simulate app activity logs from an imaginary music streaming app based on configuration settings.

The data is  sourced from S3 buckets on aws
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data

## AdditionalSteps
- one can run some analytics through table joins to get data such as:
- artist demographics and listening time(duration) to highlight the popular artist in each location 
- Help the startup assess the percentage of users who are paid subscribers by running a select count of users are at the level ('paid') and level('free').  