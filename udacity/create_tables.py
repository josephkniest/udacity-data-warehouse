from boto3.session import Session
import boto3
import json
import configparser
import psycopg2

def aws_module(resource):
    file = open('./creds.json', "r")
    creds = json.loads(file.read())
    file.close()
    return boto3.resource(resource, aws_access_key_id=creds['keyId'], aws_secret_access_key=creds['keySec'])

def connect_redshift():
    config = configparser.ConfigParser()
    config.read('redshift.cfg')
    return psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))

def reset_tables():
    conn = connect_redshift()
    cur = conn.cursor()

    cur.execute('drop table if exists public.songplays')
    cur.execute('drop table if exists public.users')
    cur.execute('drop table if exists public.songs')
    cur.execute('drop table if exists public.artists')
    cur.execute('drop table if exists public.time')

    cur.execute("""
        create table if not exists public.users (
            user_id varchar(32) not null,
            first_name varchar(32) not null,
            last_name varchar(32) not null,
            gender varchar(32) not null,
            level varchar(32) not null,
            primary key(user_id)
        )
    """)

    cur.execute("""
        create table if not exists public.artists (
            artist_id varchar(32) not null,
            name varchar(128) not null,
            location varchar(32) not null,
            latitude integer,
            longitude integer,
            primary key(artist_id)
        )
    """)

    cur.execute("""
        create table if not exists public.songs (
            song_id varchar(32) not null,
            title varchar(128) not null,
            artist_id varchar(32) not null,
            year integer,
            duration float,
            primary key(song_id),
            foreign key(artist_id) references public.artists(artist_id)
        )
    """)

    cur.execute("""
        create table if not exists public.songplays (
            songplay_id integer not null identity(1, 1),
            start_time integer not null,
            user_id varchar(32) not null,
            level varchar(32),
            song_id varchar(32) not null,
            artist_id varchar(32) not null,
            session_id integer,
            location varchar(32),
            user_agent varchar(32),
            primary key(songplay_id),
            foreign key(user_id) references public.users(user_id),
            foreign key(artist_id) references public.artists(artist_id),
            foreign key(song_id) references public.songs(song_id)
        )
    """)

    cur.execute("""
        create table if not exists public.time (
            time_id integer not null identity(1, 1),
            start_time integer not null,
            hour integer not null,
            day integer not null,
            week integer not null,
            month integer not null,
            year integer not null,
            weekday integer not null,
            primary key(time_id)
        )
    """)

    conn.commit()
    conn.close()

def main():
    reset_tables()

if __name__ == "__main__":
    main()
