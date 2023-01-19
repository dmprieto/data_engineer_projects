class CreateSql:
    CREATE_STG_EVENTS_TABLE_SQL = ("""
    CREATE TABLE IF NOT EXISTS "staging_events"(
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
    
    CREATE_STG_SONGS_TABLE_SQL = ("""
    CREATE TABLE IF NOT EXISTS "staging_songs"(
    song_id  VARCHAR(20) PRIMARY KEY,
    num_songs INTEGER,
    artist_id VARCHAR(20),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(255),
    artist_name TEXT,
    title TEXT,
    duration DOUBLE PRECISION,
    year INTEGER)
    """)
    
    CREATE_TABLE_SONGSPLAY = ("""
    CREATE TABLE IF NOT EXISTS "songplays"(
    "songplay_id" INT IDENTITY(0,1) PRIMARY KEY,
    "start_time" BIGINT NOT NULL,
    "user_id" VARCHAR(10) NOT NULL,
    "level" VARCHAR(10),
    "song_id"  VARCHAR(20) NOT NULL,
    "artist_id" VARCHAR(20) NOT NULL,
    "session_id" INTEGER NOT NULL,
    "location" VARCHAR(255),
    "user_agent" TEXT)
    """)
    
    CREATE_TABLE_USERS = ("""
    CREATE TABLE IF NOT EXISTS "users"(
    "user_id" VARCHAR(10) PRIMARY KEY,
    "first_name" VARCHAR(50) NOT NULL,
    "last_name" VARCHAR(50) NOT NULL,
    "gender" VARCHAR(1),
    "level" VARCHAR(10))
    """)
    
    CREATE_TABLE_SONGS = ("""
    CREATE TABLE IF NOT EXISTS "songs"(
    "song_id" VARCHAR(20) PRIMARY KEY,
    "title" TEXT,
    "artist_id" VARCHAR(20),
    "year" INTEGER,
    "duration" DOUBLE PRECISION
    )
    """)
    
    CREATE_TABLE_ARTISTS = ("""
    CREATE TABLE IF NOT EXISTS "artists"(
    "artist_id" VARCHAR(20) PRIMARY KEY,
    "name" TEXT,
    "location" VARCHAR(255),
    "lattitude" DOUBLE PRECISION,
    "longitude" DOUBLE PRECISION)
    """)
    
    CREATE_TABLE_TIME = ("""
    CREATE TABLE IF NOT EXISTS "time"(
    "start_time" BIGINT PRIMARY KEY,
    "hour" INTEGER NOT NULL,
    "day" INTEGER NOT NULL,
    "week" INTEGER NOT NULL,
    "month" INTEGER NOT NULL,
    "year" INTEGER NOT NULL,
    "weekday" INTEGER NOT NULL)
    """)