import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS times"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
artist varchar,
auth varchar,
firstName varchar,
gender varchar,
iteminSession int,
lastName varchar,
length float8,
level varchar,
location varchar,
method varchar,
page varchar,
registration varchar,
sessionId varchar,
song varchar,
status bigint,
ts bigint,
userAgent varchar,
userid varchar
)
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
song_id varchar,
num_songs int,
artist_id varchar,
artist_latitude float,
artist_longitude float,
artist_location varchar,
artist_name varchar,
title varchar,
duration float8,
year int
);
""")

songplay_table_create = ("""
CREATE TABLE songplays(
songplay_id bigint identity(0,1),
start_time datetime NOT NULL,
user_id varchar NOT NULL,
level varchar,
song_id varchar NOT NULL,
artist_id varchar NOT NULL,
session_id varchar,
location varchar,
user_agent varchar,
PRIMARY KEY(songplay_id)
)
""")

user_table_create = ("""
CREATE TABLE users(
user_id varchar,
first_name varchar,
last_name varchar,
gender varchar,
level varchar,
PRIMARY KEY(user_id)
)
""")

song_table_create = ("""
CREATE TABLE songs(
song_id varchar,
title varchar,
artist_id varchar NOT NULL,
year int,
duration float,
PRIMARY KEY(song_id)
)
""")

artist_table_create = ("""
CREATE TABLE artists(
artist_id varchar,
name varchar,
location varchar,
latitude float,
longitude float,
PRIMARY KEY(artist_id)
)
""")

time_table_create = ("""
CREATE TABLE times(
start_time datetime,
hour int,
day int,
week int,
month int,
year int,
weekday varchar,
PRIMARY KEY(start_time)
)
""")

add_foreign_key_songplays = ("""
ALTER TABLE songplays
ADD FOREIGN KEY(start_time) REFERENCES times(start_time),
    FOREIGN KEY(user_id) REFERENCES users(user_id),
    FOREIGN KEY(song_id) REFERENCES songs(song_id),
    FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
""")

add_foreign_key_songs = ("""
ALTER TABLE songs
ADD FOREIGN KEY(artist_id) REFERENCES artists(artist_id)
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON {}
""").format(config.get('S3', 'LOG_DATA'),
           config.get('IAM_ROLE', 'ARN'),
           config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS JSON 'auto'
""").format(config.get('S3', 'SONG_DATA'),
           config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays(
start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
)
SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time,
                se.userid,
                se.level,
                ss.song_id,
                ss.artist_id,
                se.sessionId,
                se.location,
                se.userAgent
FROM staging_events se
JOIN staging_songs ss ON (se.artist = ss.artist_name)
                      AND (se.song = ss.title)
                      AND (se.length = ss.duration)
WHERE se.page = 'NextSong' AND
      se.sessionId NOT IN (SELECT DISTINCT session_id FROM songplays)
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT userID as user_id,
                firstName as first_name,
                lastName as last_name,
                gender,
                level
FROM staging_events
WHERE page='NextSong' AND
      user_id IS NOT NULL and
      user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
FROM staging_songs
WHERE song_id IS NOT NULL AND
      song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id,
                 artist_name as name,
                 artist_location as location,
                 artist_latitude as latitude,
                 artist_longitude as longitude
FROM staging_songs
WHERE artist_id IS NOT NULL AND
      artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
INSERT INTO times(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT T.start_time,
                EXTRACT(HOUR from T.start_time) AS hour,
                EXTRACT(DAY from T.start_time) AS day,
                EXTRACT(WEEK from T.start_time) AS week,
                EXTRACT(MONTH FROM T.start_time) AS month,
                EXTRACT(YEAR FROM T.start_time) AS year,
                EXTRACT(WEEKDAY FROM T.start_time) AS weekday
FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time FROM staging_events)
                         AS T
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
