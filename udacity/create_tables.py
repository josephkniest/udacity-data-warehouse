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
    conn.close()

def main():
    connect_redshift()
    s3 = aws_module('s3')
    bucket = s3.Bucket('scpro2-udacity-data-engineering')
    for obj in bucket.objects.all():
        print(obj.key, '->', obj.get()['Body'].read())

if __name__ == "__main__":
    main()
