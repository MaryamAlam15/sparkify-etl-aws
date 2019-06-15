"""
helper file to contain all the queries.
"""

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

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar,
    auth varchar,
    firstName varchar, 
    gender varchar,
    itemInSession int,
    lastName varchar,
    length float8,
    level varchar,
    location varchar,
    method varchar,
    page varchar, 
    registration varchar,
    sessionId int,
    song varchar,
    status int,
    ts varchar,
    userAgent varchar,
    userId int
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id varchar,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration float,
    year int
    );
""")

songplays_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays 
(
    songplay_id int IDENTITY(1,1) PRIMARY KEY, 
    start_time timestamp, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent varchar
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users 
(
    user_id int PRIMARY KEY,
    first_name varchar, 
    last_name varchar,
    gender varchar, 
    level varchar
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs 
(
    song_id varchar PRIMARY KEY,
    title varchar, 
    artist_id varchar NOT NULL,
    year int,
    duration int
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id varchar PRIMARY KEY,
    name varchar,
    location varchar,
    latitude float,
    longitude float
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time 
(
    start_time timestamp PRIMARY KEY,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int
);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    json {}
    compupdate off
    region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto'
    compupdate off
    region 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# INSERT RECORDS

songplay_table_insert = ("""
insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
with artist_songs as (
    select 
        ss.song_id as song_id,
        ss.title as song_title,
        ss.duration as length,
        ss.artist_id as artist_id,
        ss.name as artist_name, 
        ss.location
    from staging_songs ss 
    inner join songs s
    on ss.artist_id=s.artist_id
)
select
     timestamp 'epoch' + FLOAT8(e.ts)/1000 * interval '1 second' as start_time,
     e.userId as user_id,
     e.level,
     a.song_id,
     a.artist_id,
     e.sessionId as session_id,
     a.location,
     e.userAgent as user_agent
from staging_events e
left join artist_songs a
on e.artist=a.artist_name
where e.song=a.song_title
and e.length=a.length;
""")

user_table_insert = ("""
begin;
    update users
    set 
        user_id = s.userId,
        first_name=s.firstName,
        last_name=s.lastName,
        gender=s.gender,
        level=s.level
    from staging_events s
    where users.user_id=s.userId;
    
    insert into users(
        user_id, 
        first_name, 
        last_name, 
        gender, 
        level
    )
    select
        s.userId as user_id,
        s.firstName as first_name,
        s.lastName as last_name,
        s.gender,
        s.level
    from staging_events s
    left join users
    on s.userId=users.user_id
    where users.user_id is null
    and s.page='NextPage';
end;
""")

song_table_insert = ("""
insert into songs (
    song_id, 
    title, 
    artist_id, 
    year, 
    duration
)
select 
    s.song_id, 
    s.title, 
    s.artist_id, 
    s.year, 
    s.duration
from staging_songs s
left outer join songs
on s.song_id=songs.song_id
where songs.song_id is null;
""")

artist_table_insert = ("""
insert into artists (
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude
)
select 
    s.artist_id, 
    s.artist_name as name, 
    s.artist_location as location, 
    s.artist_latitude as latitude, 
    s.artist_longitude as longitude
from staging_songs s
left join artists
on s.artist_id=artists.artist_id
where artists.artist_id is null;
""")

time_table_insert = ("""
insert into time (
    start_time, 
    hour, 
    day, 
    week, 
    month, 
    year, 
    weekday
)
select 
    timestamp 'epoch' + FLOAT8(ts)/1000 * interval '1 second' AS start_time,
    extract(hour from start_time) as hour,
    extract(day from start_time) as day,
    extract(week from start_time) as week,
    extract(month from start_time) as month,
    extract(year from start_time) as year,
    extract(dow from start_time) as week_day
from staging_events;
""")

# FIND SONGS

song_select = ("""
select s.song_id, s.artist_id
from songs s
inner join artists a
on a.artist_id=s.artist_id
where s.title=%s
and a.name=%s
and s.duration=%s;
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create,
    staging_songs_table_create,
    songplays_table_create,
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
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert,
    songplay_table_insert
]
