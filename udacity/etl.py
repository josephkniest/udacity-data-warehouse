from create_tables import connect_redshift, aws_module
import json

def create_temp_tables(conn):
    cur = conn.cursor()
    cur.execute("""
        create temp table if not exists stage_songs (
            artist_id varchar(32) not null,
            name varchar(128) not null,
            location varchar(32) not null,
            latitude integer not null,
            longitude integer not null,
            song_id varchar(32) not null,
            title varchar(128) not null,
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
    print(song)
    cur = conn.cursor()

    sql = """
        insert into public.artists (artist_id, name, location, lattitude, longitude)
        values ('{}', '{}', '{}', {}, {})
    """.format(
        song['artist_id'],
        song['artist_name'],
        song['artist_location'],
        0 if song['artist_latitude'] is None else song['artist_latitude'],
        0 if song['artist_longitude'] is None else song['artist_longitude'])

    print(sql)
    cur.execute(sql)

    sql = """
        insert into public.songs (song_id, title, artist_id, year, duration)
        values ('{}', '{}', '{}', {}, {})
    """.format(
        song['song_id'],
        song['title'].replace("'", "''"),
        song['artist_id'],
        0 if song['year'] is None else song['year'],
        0 if song['duration'] is None else song['duration'])

    print(sql)
    cur.execute(sql)


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

def main():
    conn = connect_redshift()
    create_temp_tables(conn)
    s3 = aws_module('s3')
    bucket = s3.Bucket('scpro2-udacity-data-engineering')
    for obj in bucket.objects.all():
        #if "song_data" in obj.key:
            #process_song(obj.get()['Body'].read(), conn)
        if "log_data" in obj.key:
            process_log(obj.get()['Body'].read().decode("utf-8"), conn)


    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
