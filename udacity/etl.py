from create_tables import connect_redshift, aws_module
import json

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
    print(file)

def main():
    conn = connect_redshift()
    s3 = aws_module('s3')
    bucket = s3.Bucket('scpro2-udacity-data-engineering')
    for obj in bucket.objects.all():
        if "song_data" in obj.key:
            process_song(obj.get()['Body'].read(), conn)
        #if "log_data" in obj.key:
            #process_log(obj.get()['Body'].read())


    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
