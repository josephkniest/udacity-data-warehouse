from create_tables import connect_redshift, aws_module
import json

def process_song(file, conn):
    song = json.loads(file)
    print(song)
    cur = conn.cursor()
    cur.execute('select * from pg_namespace')
    for rec in cur:
        print(rec)

    cur.execute("""
        insert into public.artists
        values ({}, {}, {}, {}, {})
    """.format(song['artist_id'], song['artist_name'], song['artist_location'], song['artist_latitude'], song['artist_longitude']))

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

    conn.close()

if __name__ == "__main__":
    main()
