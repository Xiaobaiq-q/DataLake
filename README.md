# Data Lake
## Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project Description
In this project, we'll apply what we've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, you will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.

## Datasets

There are two datasets for this project: song and log datasets in S3 public bucket. 

## Database Schema

#### Fact Table

| songplays |
| --- |
| songplay_id |
| start_time |
| user_id |
| level |
| song_id |
| artist_id |
| session_id |
| location |
| user_agent |

#### Dimension Tables

| users  |
| --- |
| user_id |
| first_name |
| last_name |
| gender |
| level |


| songs   |
| --- |
| song_id |
| title |
| artist_id |
| year |
| duration |


| artists    |
| --- |
| artist_id |
| name |
| location |
| lattitude |
| longitude |


| time     |
| --- |
| start_time |
| hour |
| day |
| week |
| month |
| year |
| weekday |

## ETL pipeline

1  Process the song files and create songs and artists tables. 
2  Store artist and songs folders in S3 bucket.
3  Process the Log files and filter it by the NextSong action. 
4  Create users, times and songplays table and store them in S3 bucket.