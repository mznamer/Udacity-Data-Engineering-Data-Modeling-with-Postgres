songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
    (user_id INT PRIMARY KEY, 
    first_name VARCHAR NOT NULL, 
    last_name VARCHAR NOT NULL, 
    gender CHAR(1), 
    level VARCHAR NOT NULL
    )
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
    (song_id VARCHAR PRIMARY KEY, 
    title VARCHAR NOT NULL, 
    artist_id VARCHAR NOT NULL, 
    year SMALLINT, 
    duration NUMERIC NOT NULL
    )
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists 
    (artist_id VARCHAR PRIMARY KEY, 
    name VARCHAR NOT NULL,
    location VARCHAR, 
    latitude NUMERIC, 
    longitude NUMERIC
    )
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time BIGINT PRIMARY KEY, 
    hour SMALLINT, 
    day SMALLINT,
    week SMALLINT, 
    month SMALLINT, 
    year SMALLINT, 
    weekday SMALLINT
    )
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
    (songplay_id SERIAL PRIMARY KEY, 
    start_time BIGINT, 
    user_id INT REFERENCES users(user_id) NOT NULL, 
    level VARCHAR, 
    song_id  VARCHAR, 
    artist_id  VARCHAR, 
    session_id VARCHAR NOT NULL, 
    location VARCHAR, 
    user_agent VARCHAR
    )
""")

songplay_table_insert = ("""
INSERT INTO songplays ("start_time", "user_id", "level", "song_id", "artist_id", "session_id", "location", "user_agent")
VALUES(%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO users ("user_id", "first_name", "last_name", "gender", "level")
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT (user_id)
DO 
    UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
INSERT INTO songs ("song_id", "title", "artist_id", "year", "duration")
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT songs_pkey 
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists ("artist_id", "name", "location", "latitude", "longitude")
VALUES(%s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT artists_pkey 
DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time ("start_time", "hour", "day", "week", "month", "year", "weekday")
VALUES(%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT time_pkey 
DO NOTHING;;
""")

song_select = ("""
SELECT s.song_id, s.artist_id
FROM songs s INNER JOIN artists a
ON s.artist_id = a.artist_id
WHERE s.title = %s AND s.duration = %s and a.name = %s;
""")

create_table_queries = [user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [user_table_drop, song_table_drop, artist_table_drop, time_table_drop, songplay_table_drop]
