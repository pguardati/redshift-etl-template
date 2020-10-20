import configparser
from sparkify_redshift_db.constants import CONFIG_PATH_DWH_CURRENT

# CONFIG
config = configparser.ConfigParser()
config.read(CONFIG_PATH_DWH_CURRENT)

# TABLES
star_tables = ["songplays", "users", "songs", "artists", "time"]
staging_tables = ["staging_events", "staging_songs"]

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist 		VARCHAR, 
    auth 		VARCHAR,
    firstName 		VARCHAR,
    gender 		VARCHAR,
    itemInSession 	INT,
    lastName 		VARCHAR,
    length 		FLOAT,
    level 		VARCHAR,
    location 		VARCHAR,
    method 		VARCHAR,
    page 		VARCHAR,
    registration 	FLOAT,
    sessionId 		INT,
    song 		VARCHAR,
    status 		INT,
    ts 			BIGINT, 
    userAgent 		VARCHAR,
    userId 		INT
)
diststyle key
DISTKEY (artist)
SORTKEY (ts)
;""")
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    artist_id 		VARCHAR,
    artist_latitude 	FLOAT,
    artist_location 	VARCHAR,
    artist_longitude 	FLOAT,
    artist_name 	VARCHAR, 
    duration 		FLOAT,
    num_songs 		INT,
    song_id 		VARCHAR,
    title 		VARCHAR,
    year 		INT
)
diststyle key
DISTKEY (artist_name)
;""")
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id 	BIGINT IDENTITY(0,1) PRIMARY KEY,
    start_time 		TIMESTAMP 	NOT NULL,
    user_id 		INT 		NOT NULL,
    level 		VARCHAR 	NOT NULL,
    song_id 		VARCHAR 	NOT NULL,
    artist_id 		VARCHAR 	NOT NULL,
    session_id 		INT 		NOT NULL,
    location 		VARCHAR 	NULL,
    user_agent 		VARCHAR 	NOT NULL
)
diststyle even
SORTKEY (start_time)
;""")
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id 		INT PRIMARY KEY,
    first_name 		VARCHAR 	NULL,
    last_name 		VARCHAR 	NULL,
    gender 		VARCHAR 	NULL,
    level 		VARCHAR 	NOT NULL
)
diststyle all
SORTKEY (user_id)
;""")
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id 		VARCHAR PRIMARY KEY,
    title 		VARCHAR 	NOT NULL,
    artist_id 		VARCHAR 	NOT NULL,
    year 		INT 		NULL,
    duration 		FLOAT 		NULL
)
diststyle all
SORTKEY (song_id)
;""")
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id 		VARCHAR PRIMARY KEY,
    name 		VARCHAR 	NOT NULL,
    location 		VARCHAR 	NULL,
    latitude 		FLOAT 		NULL,
    longitude 		FLOAT 		NULL
)
diststyle all 
SORTKEY (artist_id)
;""")
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time 		TIMESTAMP PRIMARY KEY,
    hour 		INT 		NOT NULL,
    day 		INT 		NOT NULL,
    week 		INT 		NOT NULL,
    month 		INT 		NOT NULL,
    year 		INT 		NOT NULL,
    weekday 		VARCHAR 	NOT NULL
)
diststyle all
SORTKEY (start_time)
;""")

# STAGING TABLES
staging_events_copy = ("""COPY staging_events FROM '{}'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON '{}'
REGION 'us-west-2';
""").format(
    config.get("S3", "log_data"),
    config.get("IAM_ROLE", "arn"),
    config.get("S3", "log_jsonpath")
)
staging_songs_copy = ("""COPY staging_songs FROM '{}'
CREDENTIALS 'aws_iam_role={}'
FORMAT AS JSON 'auto'
REGION 'us-west-2';
;""").format(
    config.get("S3", "song_data"),
    config.get("IAM_ROLE", "arn")
)

# STAR TABLES - sql2sql
songplay_table_insert = ("""
INSERT INTO songplays (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
)
SELECT DISTINCT
    TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time, 
    e.userid AS user_id, 
    e.level AS level, 
    s.song_id AS song_id, 
    s.artist_id AS artist_id, 
    e.sessionid AS session_id, 
    e.location AS location, 
    e.useragent AS user_agent
FROM staging_songs AS s
JOIN staging_events AS e
ON  e.artist = s.artist_name AND e.song = s.title
WHERE   
    e.page = 'NextSong' AND
    e.ts IS NOT NULL AND
    e.userid IS NOT NULL AND
    e.level IS NOT NULL AND
    e.sessionid IS NOT NULL AND
    e.useragent IS NOT NULL AND
    s.song_id IS NOT NULL AND
    s.artist_id IS NOT NULL 
ORDER BY start_time
;""")
user_table_insert = """
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    gender,
    level
)
SELECT 
    DISTINCT (l.userid) AS user_id,
    l.firstname AS first_name,
    l.lastname AS last_name,
    l.gender AS gender,
    l.level AS level
FROM staging_events AS l
WHERE
    l.userid IS NOT NULL AND l.userid >= 0 AND
    l.level IS NOT NULL AND
    l.page = 'NextSong'
"""
song_table_insert = ("""
INSERT INTO songs (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT 
    DISTINCT (s.song_id) as song_id,
    s.title as title,
    s.artist_id as artist_id,
    s.year as year,
    s.duration as duration
FROM staging_songs AS s
WHERE 
    s.song_id IS NOT NULL AND 
    s.title IS NOT NULL AND 
    s.artist_id IS NOT NULL AND
    s.year > 0 AND
    s.duration > 0
ORDER BY s.song_id
""")
artist_table_insert = ("""
INSERT INTO artists(
    artist_id,
    name, 
    location, 
    latitude, 
    longitude
)
SELECT 
    DISTINCT (s.artist_id) as artist_id,
    s.artist_name AS name, 
    s.artist_location AS location,
    artist_latitude as latitude,
    artist_longitude as longitude
FROM staging_songs AS s
WHERE 
    s.artist_id IS NOT NULL AND
    s.artist_name IS NOT NULL
ORDER BY artist_id
""")
time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
)
SELECT 
    DISTINCT (tmp.start_time)            AS stat_time,
    EXTRACT(hour FROM tmp.start_time)    AS hour,
    EXTRACT(day FROM tmp.start_time)     AS day,
    EXTRACT(week FROM tmp.start_time)    AS week,
    EXTRACT(month FROM tmp.start_time)   AS month,
    EXTRACT(year FROM tmp.start_time)    AS year,
    TO_CHAR(tmp.start_time, 'Day')       AS weekday 
FROM (
    SELECT /* Convert milliseconds to date */
        TIMESTAMP 'epoch' + (staging_events.ts / 1000) * INTERVAL '1 second' AS start_time
    FROM staging_events
    WHERE staging_events.ts > 0
) AS tmp
""")

# QUERY LISTS
create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplay_table_create,
    user_table_create,
    song_table_create,
    artist_table_create,
    time_table_create
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
