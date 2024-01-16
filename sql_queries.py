import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    itemInSession INTEGER,
    lastName TEXT,
    length FLOAT8,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration FLOAT8,
    sessionId INTEGER,
    song TEXT,
    status INTEGER,
    ts BIGINT,
    userAgent TEXT,
    userId TEXT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    song_id TEXT, 
	num_songs INTEGER, 
	title TEXT, 
	artist_name TEXT, 
	artist_latitude FLOAT8, 
	year INTEGER, 
	duration FLOAT8, 
	artist_id TEXT, 
	artist_longitude FLOAT8, 
	artist_location TEXT
);
""")

songplay_table_create = ("""
create table songplays (
    songplay_id BIGINT identity(0,1) primary key, 
    start_time TIMESTAMP not null, 
    user_id TEXT not null, 
    level TEXT, 
    song_id TEXT, 
    artist_id TEXT, 
    session_id INTEGER, 
    location TEXT, 
    user_agent TEXT
);
""")

user_table_create = ("""
create table users (
    user_id TEXT primary key , 
    first_name TEXT, 
    last_name TEXT, 
    gender VARCHAR(2), 
    level TEXT
);
""")

song_table_create = ("""
create table songs (
    song_id TEXT primary key, 
    title TEXT, 
    artist_id TEXT, 
    year INTEGER, 
    duration FLOAT8
);
""")

artist_table_create = ("""
create table artists (
    artist_id TEXT primary key, 
    name TEXT, 
    location TEXT, 
    latitude FLOAT8, 
    longitude FLOAT8
);
""")

time_table_create = ("""
create table time (
    start_time TIMESTAMP primary key, 
    hour INTEGER, 
    day INTEGER, 
    week INTEGER, 
    month INTEGER, 
    year INTEGER, 
    weekday INTEGER
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
FORMAT AS JSON {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
FORMAT AS JSON 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select distinct(date_add('ms',se.ts,'1970-01-01')) as start_time, 
	se.userId, 
	se.level, 
	ss.song_id,
    ss.artist_id,
    se.sessionId as session_id,
    se.location,
    se.userAgent as user_agent  
from staging_events se left join staging_songs ss 
	on (se.song = ss.title 
	and se.artist = ss.artist_name  
	and se.length = ss.duration)
where se.page='NextSong';
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
select distinct ss.userId,
	ss.firstName as first_name,
	ss.lastName as last_name,
	ss.gender,
	ss.level
from staging_events ss;
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
select distinct ss.song_id,
	ss.title,
	ss.artist_id,
	ss.year,
	ss.duration
from staging_songs ss;
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude)
select distinct ss.artist_id,
	ss.artist_name as name,
	ss.artist_location as location,
	ss.artist_latitude as latitude,
	ss.artist_longitude as longitude
from staging_songs ss;
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
select distinct(date_add('ms', se.ts,'1970-01-01')) as start_time,
	extract(hour from start_time) as hour,
	extract(day from start_time) as day,
	extract(week from start_time) as week,
	extract(month from start_time) as month,
	extract(year from start_time) as year,
	extract(weekday from start_time) as weekday
from staging_events se;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

example_question = "what's the percentage of women and men who uses Sparkify for each day of the week"
example_query = """
with tb_male as ( select t.weekday, count(distinct sp.user_id) as male_users  
from songplays sp 
	join users u
		on (sp.user_id = u.user_id 
			and sp.level = u.level)
	join time t 
		on (sp.start_time = t.start_time) 
where u.gender = 'M'
group by 1 
order by 1 ), 
tb_female as ( select t.weekday, count(distinct sp.user_id) as female_users  
from songplays sp 
	join users u
		on (sp.user_id = u.user_id 
			and sp.level = u.level)
	join time t 
		on (sp.start_time = t.start_time) 
where u.gender = 'F'
group by 1 
order by 1 )
select tb_male.weekday, 
	tb_male.male_users, 
	tb_female.female_users, 
	tb_male.male_users + tb_female.female_users as total_users, 
	cast(tb_male.male_users as float)/total_users as male_percentage, 
	cast(tb_female.female_users as float)/total_users as female_percentage
from tb_male, tb_female 
where tb_male.weekday=tb_female.weekday;
"""

example_answer = """
weekday	male_users	female_users	total_users	male_percentage	female_percentage
1	28	31	59	1	28	31	59	0.4745762711864407	0.5254237288135594
6	19	26	45	0.4222222222222222	0.5777777777777777
2	24	33	57	0.42105263157894735	0.5789473684210527
3	27	33	60	0.45	0.55
4	25	31	56	0.44642857142857145	0.5535714285714286
0	19	20	39	0.48717948717948717	0.5128205128205128
5	28	35	63	0.4444444444444444	0.5555555555555556
"""