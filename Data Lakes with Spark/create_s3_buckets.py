import boto3
import os
import glob
import configparser
from pathlib import Path


def create_s3_bucket(bucket_root, keys):
    """Create the bucket root and the folders in AWS S3
    bucket_root(string): The name of the bucket to be created
    keys(list): a list of folder_names(string) to create within the bucket
    """

    # Create the root bucket
    
    s3.create_bucket(ACL='private',Bucket=bucket_root,
                             CreateBucketConfiguration={'LocationConstraint':'us-west-2'})
    
    buck = s3.Bucket(bucket_root)

    # Create folders for the tables
    for key in keys:
        key_name = f'{key}/'
        try:
            buck.put_object(Key=key_name)
        except Exception as e:
            raise e
    print('Keys created')


def create_s3_data():
    """ Create S3 data in bucket and upload dataset's

    Returns:
            No return values
    """

    # Create Bucket
    s3.create_bucket(ACL='private',Bucket="udacity-fabio-haider",
                             CreateBucketConfiguration={'LocationConstraint':'us-west-2'})
    
    # Upload Log Dataset
    print("-- Upload Log Dataset")
    upload_dir(localdir="data/log_data/",
               bucketName="udacity-fabio-haider",
               tag='*json',
               prefix='/')

    # Upload Song Dataset
    print("-- Upload Song Dataset")
    upload_dir(localdir="data/song_data/",
               bucketName="udacity-fabio-haider",
               tag='*json',
               prefix='/')


def upload_dir(localdir, bucketName, tag, prefix):
    """
    from current working directory, upload a 'localDir' with all its subcontents (files and subdirectories...)
    to a aws bucket
    Parameters
    ----------
    localdir :   localDirectory to be uploaded, with respect to current working directory
    bucketName : bucket in aws
    tag :        tag to select files, like *png
                 NOTE: if you use tag it must be given like --tag '*txt', in some quotation marks... for argparse
    prefix :     to remove initial '/' from file names

    Returns
    -------
    None
    """

    cwd = str(Path.cwd())
    p = Path(os.path.join(Path.cwd(), localdir))
    mydirs = list(p.glob('**'))
    file_count = int(0)

    for mydir in mydirs:
        filenames = glob.glob(os.path.join(mydir, tag))
        filenames = [f for f in filenames if not Path(f).is_dir()]
        for i, filename in enumerate(filenames):
            filename = str(filename).replace(cwd, '')
            if filename.startswith(prefix):
                filename = filename.replace(prefix, "", 1)
            file_count = file_count + int(1)
            s3.meta.client.upload_file(filename, bucketName, filename)

    print("-- Total files uploaded --> {}".format(file_count))


if __name__ == '__main__':
    # Load AWS credentials as env vars
    config = configparser.ConfigParser()
    config.read_file(open('config/dl.cfg'))
    
    # Create client for S3
    s3 = boto3.resource('s3',region_name = 'us-west-2',
                     aws_access_key_id = config['AWS']['AWS_ACCESS_KEY_ID'],
                     aws_secret_access_key = config['AWS']['AWS_SECRET_ACCESS_KEY'])

    print("-- Create S3 Bucket")
    create_s3_bucket(bucket_root='udacity-fabio-haider-sparkify-data-lake',
                     keys=['songs', 'songplays', 'time', 'artists', 'users'])

    print("-- Create Datasets in Bucket")
    create_s3_data()

    print("-- Process Terminated without errors")
