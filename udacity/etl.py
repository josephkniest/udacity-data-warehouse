from create_tables import connect_redshift, aws_module

def main():
    s3 = aws_module('s3')
    bucket = s3.Bucket('scpro2-udacity-data-engineering')
    for obj in bucket.objects.all():
        print(obj.key, '->', obj.get()['Body'].read())

if __name__ == "__main__":
    main()
