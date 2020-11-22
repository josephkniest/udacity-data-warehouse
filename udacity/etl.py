from create_tables import connect_redshift, aws_module
import json

def process_song(file):
    song = json.loads(file)
    print(song)

def process_log(file):
    print(file)

def main():
    s3 = aws_module('s3')
    bucket = s3.Bucket('scpro2-udacity-data-engineering')
    for obj in bucket.objects.all():
        if "song_data" in obj.key:
            process_song(obj.get()['Body'].read())
        #if "log_data" in obj.key:
            #process_log(obj.get()['Body'].read())

if __name__ == "__main__":
    main()
