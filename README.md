#### Purpose

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

Purpose of this project is to build an etl pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.


#### Project Datasets

Song data: s3://udacity-dend/song_data
Log data: s3://udacity-dend/log_data
Log data json path: s3://udacity-dend/log_json_path.json

- Song Dataset
    Each file is in JSON format and contains metadata about a song and the artist of that song. 
- Log Dataset
    The second dataset consists of log files in JSON format.The log files in the dataset are   partitioned by year and month. 
    
#### Schema

- Fact table:
    - Songplay:
        - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

- Dimension tables:
    - users: users in the app
        - user_id, first_name, last_name, gender, level
    - songs: songs in music database
        - song_id, title, artist_id, year, duration
    - artists: artists in music database
        - artist_id, name, location, lattitude, longitude
    - time:timestamps of records in songplays broken down into specific units
        - start_time, hour, day, week, month, year, weekday
       
#### Files and Scripts

- dwh.cfg
    Configuration file. Please enter you aws access key and secret in the KEY and SECRET fields under [AWS]
- create_cluster.py
    Creates redshift cluster and iam role
    Ouputs the DWH_Endpoint and DWH_ROLE_ARN
    Please update dwh.cfg with Host=DWH_Endpoint_Value and DWH_ROLE_ARN=DWH_ROLE_ARN_VALUE before running the create_tables.py and etl.py scripts.
- destroy_cluster.py
    Destroys redshift cluster and iam role
- sql_queries.py
    Contains sql queries for creating tables, loading tables, inserting data in fact and dimension tables along with dropping tables.
- create_tables.py
    Drops any existing tables from the database. Creates the staging tables, fact and dimension tables
- etl.py
    Copies data in staging tables from s3.
    Inserts data in fact and dimension tables.