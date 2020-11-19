from boto3.session import Session
import boto3
import json

def main():
    file = open('./creds.json', "r")
    creds = json.loads(file.read())
    file.close()
    s3 = boto3.client('s3', aws_access_key_id=creds['keyId'], aws_secret_access_key=creds['keySec'])
    print(s3.list_objects(Bucket='scpro2-udacity-data-engineering'))

if __name__ == "__main__":
    main()
