import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stage_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS stage_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS stage_events(
event_id INT IDENTITY(0,1),
artist VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender  VARCHAR,
itemInSession INTEGER,
lastName VARCHAR,
length NUMERIC, 
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
sessionId INT,
song VARCHAR,
status INTEGER,
ts BIGINT,
userAgent VARCHAR,	
userId VARCHAR,
PRIMARY KEY (event_id)
)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS stage_songs(
num_songs int,
artist_id varchar,
artist_latitude numeric,
artist_longitude numeric,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration numeric,
year int
)""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
songplay_id int IDENTITY(0,1) PRIMARY KEY,
start_time timestamp NOT NULL,
user_id varchar NOT NULL, 
level varchar,
song_id varchar,
artist_id varchar,
session_id varchar NOT NULL,
location varchar,
user_agent varchar
)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
user_id varchar PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
song_id varchar PRIMARY KEY,
title varchar,
artist_id varchar,
year int,
duration numeric
)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
artist_id varchar PRIMARY KEY,
name varchar,
location varchar,
latitude numeric,
longitude numeric
)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
start_time timestamp PRIMARY KEY,
hour int,
day int,
week int,
month int,
year int,
weekday int
)""")

# STAGING TABLES
#use LOG_JSONPATH to get correct column mapping
staging_events_copy = ("""
copy stage_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON {};
""").format(config.get("IAM_ROLE","ARN"), config.get('S3','LOG_JSONPATH'))

#straight load from s3 into table
staging_songs_copy = ("""
copy stage_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role={}'
    region 'us-west-2' compupdate off 
    JSON 'auto' truncatecolumns;
""").format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
#all inserts are done table to table instead of using %S

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
(TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second'),
e.userid,
e.level,
s.song_id,
s.artist_id,
e.sessionId,
e.location,
e.useragent
FROM stage_events e JOIN stage_songs s
ON (e.artist=s.artist_name AND
e.song=s.title AND
e.length=s.duration)
where e.page = 'NextSong'""")


user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
select DISTINCT
userid,
firstname,
lastname,
gender,
level
from stage_events""")



song_table_insert = ("""INSERT INTO songs(song_id,title,artist_id,year,duration)
SELECT DISTINCT
song_id,
title,
artist_id,
year,
duration
from stage_songs""")


artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT
s.artist_id,
s.artist_name,
e.location,
s.artist_latitude,
s.artist_longitude
FROM stage_events e JOIN stage_songs s
ON (e.artist=s.artist_name AND
e.song=s.title AND
e.length=s.duration)""")



time_table_insert = ("""INSERT INTO time(start_time,hour,day,week,month,year,weekday)
SELECT DISTINCT ts as timestamp,
EXTRACT (HOUR FROM ts), 
EXTRACT (DAY FROM ts),
EXTRACT (WEEK FROM ts), 
EXTRACT (MONTH FROM ts),
EXTRACT (YEAR FROM ts), 
EXTRACT (WEEKDAY FROM ts) FROM
(SELECT TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as ts FROM stage_events);""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
