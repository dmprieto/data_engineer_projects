class SqlQueries:
    songplays_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
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

    users_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level) 
        SELECT DISTINCT se.user_id,
            se.user_first_name,
            se.user_last_name,
            se.user_gender,
            se.user_level
        FROM staging_events se 
        WHERE se.page = 'NextSong'
    """)

    songs_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
        SELECT DISTINCT ss.song_id,
            ss.title,
            ss.artist_id,
            ss.year,
            ss.duration
        FROM staging_songs ss 
    """)

    artists_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, lattitude, longitude) 
        SELECT DISTINCT ss.artist_id,
            ss.artist_name,
            ss.artist_location,
            ss.artist_latitude,
            ss.artist_longitude
        FROM staging_songs ss 
    """)

    time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday) 
        SELECT DISTINCT EXTRACT(EPOCH FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS start_time, 
            EXTRACT(HOUR FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS hour, 
            EXTRACT(DAY FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS day, 
            EXTRACT(WEEK FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS week, 
            EXTRACT(MONTH FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS month, 
            EXTRACT(YEAR FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS year, 
            EXTRACT(WEEKDAY FROM(TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 Second ')) AS weekday  
        FROM staging_events
    """)