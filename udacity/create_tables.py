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
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    cur.execute('drop table if exists songplays')
    cur.execute('drop table if exists users')
    cur.execute('drop table if exists songs')
    cur.execute('drop table if exists artists')
    cur.execute('drop table if exists time')

    cur.execute("""
        create table if not exists users (
            user_id varchar(16) not null,
            first_name varchar(16) not null,
            last_name varchar(16) not null,
            gender varchar(16) not null,
            level varchar(16) not null,
            primary key(user_id)
        )
    """)

    cur.execute("""
        create table if not exists artists (
            artist_id varchar(16) not null,
            name varchar(16) not null,
            location varchar(16) not null,
            lattitude integer not null,
            longitude integer not null,
            primary key(artist_id)
        )
    """)

    cur.execute("""
        create table if not exists songs (
            song_id varchar(16) not null,
            title varchar(16) not null,
            artist_id varchar(16) not null,
            year integer,
            duration float,
            primary key(song_id),
            foreign key(artist_id) references artists(artist_id)
        )
    """)

    cur.execute("""
        create table if not exists songplays (
            songplay_id integer not null,
            start_time integer not null,
            user_id varchar(16) not null,
            level varchar(16),
            song_id varchar(16) not null,
            artist_id varchar(16) not null,
            session_id integer,
            location varchar(16),
            user_agent varchar(16),
            primary key(songplay_id),
            foreign key(user_id) references users(user_id),
            foreign key(artist_id) references artists(artist_id),
            foreign key(song_id) references songs(song_id)
        )
    """)

    cur.execute("""
        create table if not exists time (
            time_id integer not null,
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

    conn.close()

def main():
    connect_redshift()
    s3 = aws_module('s3')
    #bucket = s3.Bucket('scpro2-udacity-data-engineering')
    #for obj in bucket.objects.all():
        #print(obj.key, '->', obj.get()['Body'].read())

if __name__ == "__main__":
    main()
