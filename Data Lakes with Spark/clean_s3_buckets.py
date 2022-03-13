import boto3
import configparser
     

if __name__=='__main__':
    # load Params from a file
    config = configparser.ConfigParser()
    config.read_file(open('config/dl.cfg'))
    AWS_ACCESS_KEY_ID = config.get('AWS','AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = config.get('AWS','AWS_SECRET_ACCESS_KEY')
    
    # Create client for S3
    s3 = boto3.resource('s3',region_name = 'us-west-2',
                     aws_access_key_id = AWS_ACCESS_KEY_ID,
                     aws_secret_access_key = AWS_SECRET_ACCESS_KEY)
    
    # Delete Buckets
    print("-- Delete Buckets")
    bucket = s3.Bucket("udacity-fabio-haider-sparkify-data-lake")
    bucket.objects.all().delete()
    bucket.delete()

    bucket = s3.Bucket("udacity-fabio-haider")
    bucket.objects.all().delete()
    bucket.delete()
    
    print("-- Process Terminated without errors")