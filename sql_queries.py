import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""
    create table if not exists staging_events(
        artist varchar,
        auth varchar,
        first_name varchar,
        gender varchar,
        item_insession integer,
        last_name varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        session_id integer,
        song varchar,
        status integer,
        ts varchar,
        user_agent varchar,
        user_id varchar);  
""")

staging_songs_table_create = ("""
    create table if not exists staging_songs(
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int)
""")

songplay_table_create = ("""
    create table if not exists songplays(
        songplay_id int identity(0,1) primary key,
        start_time timestamp not null,
        user_id varchar,
        level varchar,
        song_id varchar not null,
        artist_id varchar not null,
        session_id int,
        location varchar,
        user_agent varchar);
""")

user_table_create = ("""
    create table if not exists users(
        user_id varchar primary key, 
        first_name varchar,
        last_name varchar,
        gender varchar,
        level varchar);
""")

song_table_create = ("""
    create table if not exists songs (
        song_id varchar primary key,
        title varchar not null,
        artist_id varchar,
        year int,
        duration float);
""")

artist_table_create = ("""
    create table if not exists artists(
        artist_id varchar primary key,
        name varchar,
        location varchar,
        latitude float,
        longitude float);
""")

time_table_create = ("""
    create table if not exists time(
        start_time timestamp primary key,
        hour int,
        day int,
        week int,
        month int,
        year int, 
        weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {} 
iam_role {}
FORMAT AS JSON {};
""").format(
    config.get('S3', 'LOG_DATA'), 
    config.get('IAM_ROLE', 'ARN'), 
    config.get('S3', 'LOG_JSONPATH'))
    

staging_songs_copy = ("""
copy staging_songs from {} 
iam_role {}
FORMAT AS JSON 'auto';
""").format(
    config.get('S3', 'SONG_DATA'), 
    config.get('IAM_ROLE', 'ARN'))
    

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select timestamp 'epoch' + float8(se.ts)/1000 * interval '1 second' as start_time, se.user_id, se.level, ss.song_id, ss.artist_id,   se.session_id, se.location, se.user_agent
                from staging_events se 
                    inner join staging_songs ss
                        on (se.song = ss.title and se.artist = ss.artist_name and se.length = ss.duration ) 
                            where se.page = 'NextSong'          
""")

user_table_insert = ("""
    insert into users(user_id, first_name, last_name, gender, level)
        select se.user_id, se.first_name, se.last_name, se.gender, se.level
            from staging_events se 
                where not exists (select 1 from staging_events see where se.user_id = see.user_id and se.ts < see.ts)
""")

song_table_insert = ("""
    insert into songs(song_id, title, artist_id, year, duration)
        select ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
            from staging_songs ss 
                where   ss.song_id is not null
""")

artist_table_insert = ("""
    insert into artists(artist_id, name, location, latitude, longitude)
        select distinct ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_latitude,ss.artist_longitude
            from staging_songs ss
""")

time_table_insert = ("""
    insert into time ( start_time, hour, day, week, month, year, weekday)
        select distinct timestamp 'epoch' + FLOAT8(ts)/1000 * interval '1 second' AS start_time,
            extract(hour from start_time),
            extract(day from start_time),
            extract(week from start_time),
            extract(month from start_time),
            extract(year from start_time),
            extract(dow from start_time)
                from staging_events   
    
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
