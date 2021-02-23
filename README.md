# Project Data Modeling with Postgres
Using the song and log datasets, creates a star schema database optimized for queries on song play data analysis. 

## Files in project
sql_queries.py - defines queries for creating, dropping tables, and inserting data into tables.
create_tables.py - connects to the sparkify database, drops any tables if they exist, and creates the tables.
etl.py (main script for data processing) - connects to the sparkify database, extracts and processes the log_data and song_data, and loads data into the tables.


## DB Schema
For the database's schema was chosen star schema that allows easy getting data for analytical purposes.


## ETL process
Using song_data files we created two tables - songs and artists. 

Using log_data files we separated information about users, played songs, and created a table with extended information about time when users started to listen to songs.


## Futere Data Analysis
We can define popular artists, popular songs, active and passive users, location of poplular artists, what userAgent is used more often.

Using time table we can easily analyze weekdays, hours, weeks, and months when users were more active.

Based on users table we can pull some information about users and answer questions if we have more men or women, how many users use paid level.
