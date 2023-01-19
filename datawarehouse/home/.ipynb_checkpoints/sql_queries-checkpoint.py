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
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE "staging_events"(
    "event_id" INT IDENTITY(0,1) PRIMARY KEY,
    "artist_name" TEXT,
    "auth" VARCHAR(15),
    "user_first_name" VARCHAR(50),
    "user_gender" VARCHAR(1),
    "item_in_session" VARCHAR(5),
    "user_last_name" VARCHAR(50),
    "song_length" DOUBLE PRECISION,
    "user_level" VARCHAR(10),
    "location" VARCHAR(255),
    "method" VARCHAR(10),
    "page" VARCHAR(40),
    "registration" BIGINT,
    "session_id" INTEGER,
    "song_title" TEXT,
    "status" VARCHAR(3),
    "ts" BIGINT,
    "user_agent" TEXT,
    "user_id" VARCHAR(10)
)""")

staging_songs_table_create = ("""CREATE TABLE "staging_songs"(
    song_id  VARCHAR(20) PRIMARY KEY,
    num_songs INTEGER,
    artist_id VARCHAR(20),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name TEXT,
    title TEXT,
    duration DOUBLE PRECISION,
    year INTEGER
)
""")

songplay_table_create = ("""CREATE TABLE "songplays"(
    "songplay_id" INT IDENTITY(0,1) PRIMARY KEY,
    "start_time" BIGINT NOT NULL,
    "user_id" VARCHAR(10) NOT NULL,
    "level" VARCHAR(10),
    "song_id"  VARCHAR(20) NOT NULL,
    "artist_id" VARCHAR(20) NOT NULL,
    "session_id" INTEGER NOT NULL,
    "location" VARCHAR(255),
    "user_agent" TEXT
)
""")

user_table_create = ("""CREATE TABLE "users"(
    "user_id" VARCHAR(10) PRIMARY KEY,
    "first_name" VARCHAR(50) NOT NULL,
    "last_name" VARCHAR(50) NOT NULL,
    "gender" VARCHAR(1),
    "level" VARCHAR(10)
)
""")

song_table_create = ("""CREATE TABLE "songs"(
    "song_id" VARCHAR(20) PRIMARY KEY,
    "title" TEXT,
    "artist_id" VARCHAR(20),
    "year" INTEGER,
    "duration" DOUBLE PRECISION
)
""")

artist_table_create = ("""CREATE TABLE "artists"(
    "artist_id" VARCHAR(20) PRIMARY KEY,
    "name" TEXT,
    "location" VARCHAR(255),
    "lattitude" DOUBLE PRECISION,
    "longitude" DOUBLE PRECISION
)
""")

time_table_create = ("""CREATE TABLE "time"(
    "start_time" BIGINT PRIMARY KEY,
    "hour" INTEGER NOT NULL,
    "day" INTEGER NOT NULL,
    "week" INTEGER NOT NULL,
    "month" INTEGER NOT NULL,
    "year" INTEGER NOT NULL,
    "weekday" INTEGER NOT NULL
)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {} 
    credentials 'aws_iam_role={}' 
    compupdate off 
    region 'us-west-2'
    JSON {}
    """).format(config.get('S3','LOG_DATA'),config.get('IAM_ROLE','ARN'),config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}' 
    compupdate off 
    region 'us-west-2'
    JSON 'auto'
""").format(config.get('S3','SONG_DATA'),config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT DISTINCT EXTRACT(EPOCH FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS start_time,
    se.user_id,
    se.user_level,
    ss.song_id,
    ss.artist_id,
    se.session_id,
    se.location,
    se.user_agent
FROM staging_events se, staging_songs ss  
WHERE se.song_title = ss.title
AND se.artist_name = ss.artist_name
AND se.page = 'NextSong'
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) 
SELECT DISTINCT se.user_id,
    se.user_first_name,
    se.user_last_name,
    se.user_gender,
    se.user_level
FROM staging_events se 
WHERE se.page = 'NextSong'
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration) 
SELECT DISTINCT ss.song_id,
    ss.title,
    ss.artist_id,
    ss.year,
    ss.duration
FROM staging_songs ss 
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, lattitude, longitude) 
SELECT DISTINCT ss.artist_id,
    ss.artist_name,
    ss.artist_location,
    ss.artist_latitude,
    ss.artist_longitude
FROM staging_songs ss 
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
SELECT DISTINCT EXTRACT(EPOCH FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS start_time, 
    EXTRACT(HOUR FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS hour, 
    EXTRACT(DAY FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS day, 
    EXTRACT(WEEK FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS week, 
    EXTRACT(MONTH FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS month, 
    EXTRACT(YEAR FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS year, 
    EXTRACT(WEEKDAY FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS weekday  
FROM staging_events
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
