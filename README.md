Introduction
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Project Description
In this project, we will build an ETL pipeline for a database hosted on Redshift. we will load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

purpose of the project:
the purpose of this project is to model the data into a data warehouse that simplifies the process of query and analysis.

Schema for the project:
Using the song and log datasets to create a star schema optimized for queries on song play analysis. This includes the following tables.

Fact Table
songplays - records in log data associated with song plays i.e. records with page NextSong
songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

Dimension Tables
users - users in the app
user_id, first_name, last_name, gender, level

songs - songs in music database
song_id, title, artist_id, year, duration

artists - artists in music database
artist_id, name, location, latitude, longitude

time - timestamps of records in songplays broken down into specific units
start_time, hour, day, week, month, year, weekday

python scripts:

sql_queries.py: this file contains all queries that we use to delete previous schema if it exists, create new tables, and insert the data extracted from data files into the database.

create_tables.py: this files contains the python methods that uses sql_queries.py to delete any previous schema then create new schema with the tables we need in redshift.

etl.py: this file contains the methods that we use to extract the data from data files and load it into staging tables then insert it into DWH.

usage:
first run craete_tables.py to intiate the schema and create tables

second run etl.py to extract the data from files and ingest them into the data warehouse