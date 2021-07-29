import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists song"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= ("""\
    create table if not exists staging_events(
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession integer,
        lastName varchar,
        length double precision,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration double precision,
        sessionId integer,
        song varchar,
        status integer,
        ts bigint,
        userAgent varchar,
        userId integer  
    );
""")

staging_songs_table_create = ("""\
    create table if not exists staging_songs(
        song_id varchar,
        title varchar,
        duration numeric,
        year numeric,
        num_songs integer,
        artist_id varchar,
        artist_latitude numeric,
        artist_longitude numeric,
        artist_location varchar,
        artist_name varchar       
    );
""")

user_table_create = ("""\
    create table if not exists users(
        user_id integer not null primary key sortkey,
        first_name varchar not null,
        last_name varchar not null,
        gender varchar not null,
        level varchar not null
    )diststyle all;
""")

song_table_create = ("""\
    create table if not exists song(
        song_id varchar not null primary key,
        title varchar not null,
        artist_id varchar not null,
        year integer not null,
        duration numeric not null
    );
""")

artist_table_create = ("""\
    create table if not exists artist(
        artist_id varchar not null primary key,
        name varchar not null,
        location varchar,
        latitude numeric,
        longitude numeric
    )diststyle all;
""")

time_table_create = ("""\
    create table if not exists time(
        start_time bigint primary key sortkey,
        hour integer not null,
        day integer not null,
        week integer not null,
        month integer not null,
        year integer not null,
        weekday integer not null
    )diststyle all;
""")

songplay_table_create = ("""\
    create table if not exists songplay(
        songplay_id integer identity(0,1) primary key,
        start_time timestamp not null references time(start_time) sortkey,
        user_id integer not null references users(user_id),
        level varchar not null,
        song_id varchar not null references song(song_id) distkey,
        artist_id varchar not null references artist(artist_id),
        session_id integer not null,
        location varchar not null,
        user_agent varchar not null
    );
""")



# STAGING TABLES

staging_events_copy = ("""\
        copy staging_events from {}
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        format as json {};
""").format(config.get("S3","LOG_DATA"),
            config.get("IAM_ROLE","ROLE_ARN"),
            config.get("S3","LOG_JSONPATH"))

staging_songs_copy = ("""\
    copy staging_songs FROM {}
        credentials 'aws_iam_role={}'
        region 'us-west-2'
        format as json 'auto';
""").format(config.get("S3","SONG_DATA"),
            config.get("IAM_ROLE","ROLE_ARN"))

# FINAL TABLES

user_table_insert = ("""\
    insert into users(
        user_id,
        first_name,
        last_name, 
        gender,
        level
    ) 
    select distinct 
            userId, 
            firstName, 
            lastName, 
            gender, 
            level
    from staging_events 
    where userId is not null;
""")

song_table_insert = ("""\
    insert into song(
        song_id,
        title,
        artist_id,
        year,
        duration
    ) 
    select distinct 
            song_id, 
            title, 
            artist_id, 
            year, 
            duration 
    from staging_songs 
    where song_id is not null;
""")

artist_table_insert = ("""\
    insert into artist(
        artist_id, 
        name, 
        location, 
        latitude, 
        longitude
    )
    select distinct 
            artist_id, 
            artist_name, 
            artist_location, 
            artist_latitude, 
            artist_longitude
    from staging_songs 
    where artist_id is not null;
""")

time_table_insert = ("""\
    insert into time(
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    select start_time,
       date_part(hour, date_time) as hour,
       date_part(day, date_time) as day,
       date_part(week, date_time) as week,
       date_part(month, date_time) as month,
       date_part(year, date_time) as year,
       date_part(weekday, date_time) as weekday
    from (select ts as start_time,
           '1970-01-01'::date + ts/1000 * interval '1 second' as date_time
            from staging_events group by ts) as temp
    order by start_time;
""")

songplay_table_insert = ("""\
    insert into songplay(
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    select timestamp 'epoch' + ts * interval '0.001 seconds' as start_time, 
       userId, 
       level, 
       song.song_id as song_id, 
       artist.artist_id as artist_id,
       sessionId, 
       staging_events.location as location, 
       userAgent
    from staging_events 
    inner join artist on artist.name = staging_events.artist
    inner join song on song.title = staging_events.song 
    where page='NextSong'
    and userId not in 
        (select distinct user_id 
         from songplay sp 
         where sp.user_id = userId 
         and sp.session_id = sessionId
         and sp.start_time = start_time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]

