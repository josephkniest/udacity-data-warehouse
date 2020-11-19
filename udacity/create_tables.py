from boto3.session import Session
import boto3
import json

def load_s3():
    file = open('./creds.json', "r")
    creds = json.loads(file.read())
    file.close()
    return boto3.resource('s3', aws_access_key_id=creds['keyId'], aws_secret_access_key=creds['keySec'])

def main():
    s3 = load_s3()
    bucket = s3.Bucket('scpro2-udacity-data-engineering')
    for obj in bucket.objects.all():
        print(obj.key, '->', obj.get()['Body'].read())

if __name__ == "__main__":
    main()
