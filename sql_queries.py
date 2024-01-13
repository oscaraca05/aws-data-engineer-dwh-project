import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE staging_events;"
staging_songs_table_drop = "DROP TABLE staging_songs;"
songplay_table_drop = "DROP TABLE songplay;"
user_table_drop = "DROP TABLE user;"
song_table_drop = "DROP TABLE song;"
artist_table_drop = "DROP TABLE artist;"
time_table_drop = "DROP TABLE time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist VARCHAR(50),
    auth VARCHAR(20),
    firstName VARCHAR(30),
    gender VARCHAR(2),
    itemInSession INTEGER,
    lastName VARCHAR(30),
    length FLOAT8,
    level VARCHAR(10),
    location VARCHAR(50),
    method VARCHAR(10),
    page VARCHAR(20),
    registration FLOAT8,
    sessionId INTEGER,
    song VARCHAR(50),
    status INTEGER,
    ts FLOAT8,
    userAgent VARCHAR(100),
    userId VARCHAR(8)
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    song_id VARCHAR(20), 
	num_songs INTEGER, 
	title VARCHAR(50), 
	artist_name VARCHAR(50), 
	artist_latitude FLOAT8, 
	year INTEGER, 
	duration FLOAT8, 
	artist_id VARCHAR(20), 
	artist_longitude FLOAT8, 
	artist_location VARCHAR(50)
);
""")

songplay_table_create = ("""
CREATE TABLE songplay (
    songplay_id VARCHAR(20), 
    start_time FLOAT8, 
    user_id VARCHAR(8), 
    level VARCHAR(10), 
    song_id VARCHAR(20), 
    artist_id VARCHAR(20), 
    session_id INTEGER, 
    location VARCHAR(50), 
    user_agent VARCHAR(100)
);
""")

user_table_create = ("""
CREATE TABLE user (
    user_id VARCHAR(8), 
    first_name VARCHAR(30), 
    last_name VARCHAR(30), 
    gender VARCHAR(2), 
    level VARCHAR(10)
);
""")

song_table_create = ("""
CREATE TABLE song (
    song_id VARCHAR(20), 
    title VARCHAR(50), 
    artist_id VARCHAR(20), 
    year INTEGER, 
    duration FLOAT8
);
""")

artist_table_create = ("""
CREATE TABLE artist (
    artist_id VARCHAR(20), 
    name VARCHAR(50), 
    location VARCHAR(50), 
    latitude FLOAT8, 
    longitude FLOAT8
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time TIMESTAMP, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday VARCHAR(2)
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY tabla1
FROM 's3://bucket_name/folder_name/'
IAM_ROLE 'arn:aws:iam::1234567890:role/your_role'
FORMAT AS JSON 'auto'
""").format()

staging_songs_copy = ("""
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
to_timestamp(1541107493796/1000)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
