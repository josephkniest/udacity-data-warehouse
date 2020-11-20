from boto3.session import Session
import boto3
import json
import sqlalchemy as sa
def aws_module(resource):
    file = open('./creds.json', "r")
    creds = json.loads(file.read())
    file.close()
    return boto3.resource(resource, aws_access_key_id=creds['keyId'], aws_secret_access_key=creds['keySec'])

def connect_redshift():
    file = open('./redshift.json', "r")
    redshift = json.loads(file.read())
    file.close()
    u = 'redshift+psycopg2://redshiftuser:UdacityRedshiftProject1@redshift-udacity-scpro2.cwfaavi1fb7o.us-east-2.redshift.amazonaws.com5439/dev'
    print(sa.create_engine(u))

def main():
    connect_redshift()
    s3 = aws_module('s3')
    bucket = s3.Bucket('scpro2-udacity-data-engineering')
    for obj in bucket.objects.all():
        print(obj.key, '->', obj.get()['Body'].read())

if __name__ == "__main__":
    main()
