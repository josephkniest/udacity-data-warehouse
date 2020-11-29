from create_tables import connect_redshift, aws_module
import json

def create_temp_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        create temp table if not exists stage_songs (
            artist_id varchar(32),
            name varchar(128),
            location varchar(32),
            latitude integer,
            longitude integer,
            song_id varchar(32),
            title varchar(128),
            year integer,
            duration float
        )
    """)

    cur.execute("""
        create temp table if not exists stage_logs (
            log_id integer not null identity(1, 1),
            start_time bigint not null,
            user_id varchar(32) not null,
            level varchar(8),
            song varchar(256),
            artist varchar(256),
            session_id integer,
            location varchar(64),
            user_agent varchar(256),
            first_name varchar(32),
            last_name varchar(32),
            gender varchar(32),
            primary key(log_id)
        )
    """)


def process_song(file, conn):
    song = json.loads(file)
    cur = conn.cursor()
    print(song)
    sql = """
        insert into stage_songs (artist_id, name, location, latitude, longitude, song_id, title, year, duration)
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    cur.execute(sql, (
        None if song['artist_id'] is None else song['artist_id'].replace("'", "''"),
        None if song['artist_name'] is None else song['artist_name'].replace("'", "''"),
        None if song['artist_location'] is None else song['artist_location'].replace("'", "''"),
        None if song['artist_latitude'] is None else song['artist_latitude'],
        None if song['artist_longitude'] is None else song['artist_longitude'],
        None if song['song_id'] is None else song['song_id'].replace("'", "''"),
        None if song['title'] is None else song['title'].replace("'", "''"),
        None if song['year'] is None else song['year'],
        None if song['duration'] is None else song['duration']))


def process_log(file, conn):
    jsonS = '[' + file.replace("}", "},")
    jsonS = jsonS[:-1] + ']'
    logs = json.loads(jsonS)
    cur = conn.cursor()

    for log in logs:
        print(log)
        sql = """
            insert into stage_logs (start_time, user_id, level, song, artist, session_id, location, user_agent, first_name, last_name, gender)
            values ({}, '{}', '{}', '{}', '{}', {}, '{}', '{}', '{}', '{}', '{}')
        """.format(
            log['ts'],
            log['userId'].replace("'", "''"),
            log['level'].replace("'", "''"),
            None if log['song'] is None else log['song'].replace("'", "''"),
            None if log['artist'] is None else log['artist'].replace("'", "''"),
            log['sessionId'],
            None if log['location'] is None else log['location'].replace("'", "''"),
            None if log['userAgent'] is None else log['userAgent'].replace("'", "''"),
            None if log['firstName'] is None else log['firstName'].replace("'", "''"),
            None if log['lastName'] is None else log['lastName'].replace("'", "''"),
            None if log['gender'] is None else log['gender'].replace("'", "''"))

        cur.execute(sql)

def insert_artists_songs(conn):
    cur = conn.cursor()
    sql = """
        insert into public.artists
            select artist_id, name, location, latitude, longitude
            from stage_songs
            left join public.artists using(artist_id, name, location, latitude, longitude)
            where public.artists.artist_id is null
    """

    cur.execute(sql)

    sql = """
        insert into public.songs
            select song_id, title, artist_id, year, duration
            from stage_songs
            left join public.songs using(song_id, title, artist_id, year, duration)
            where public.songs.song_id is null
    """

    cur.execute(sql)

    conn.commit()

def main():
    conn = connect_redshift()
    create_temp_tables(conn)
    s3 = aws_module('s3')
    bucket = s3.Bucket('scpro2-udacity-data-engineering')
    for obj in bucket.objects.all():
        if "song_data" in obj.key:
            process_song(obj.get()['Body'].read(), conn)
        #if "log_data" in obj.key:
            #process_log(obj.get()['Body'].read().decode("utf-8"), conn)


    insert_artists_songs(conn)
    conn.commit()

    conn.close()

if __name__ == "__main__":
    main()
