import pandas as pd
import boto3
import json
import configparser
import time
import os
import glob
from pathlib import Path
from botocore.exceptions import ClientError


def create_iam_role(): 
    """ Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
     
    Returns:
            an IAM Role 
    """
    
    # Create an IAM role
    try:
        print("-- Creating a new IAM Role ...") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                   'Effect': 'Allow',
                   'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
            )    
    except Exception as e:
        print(e)

    print("-- Attaching Policy ...")
    iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    print("-- Get the IAM role ARN ...")
    roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']
    
    config.set('IAM','IAM_ROLE',roleArn)   
    with open('dwh.cfg', 'w') as configfile:    
        config.write(configfile)

    return roleArn


def create_redshift_cluster():
    """ Create an Redshift Cluster
     
    Returns:
            Endpoint (The DNS address of the Cluster)
    """
    
    # create client for redshift 
    host=""
    
    try:
        print("-- Creating a Redshift Cluster") 
        response = redshift.create_cluster(        
            # HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_CLUSTERS),

            # Identifiers & Credentials
            DBName=DWH_NAME,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_USER,
            MasterUserPassword=DWH_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[IAM_ROLE] 
        )
        
    except Exception as e:
        print(e)
        
    while redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']=='creating':
        print('.',end='',flush=True)
        time.sleep(10)
        
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    if myClusterProps['ClusterStatus']=='available':
        print("")
        print("-- Redshift cluster is available")
        host = myClusterProps['Endpoint']['Address']       
        myClusterProps['ClusterStatus'] == 'available'
    else:
        print("-- Creating Redshift Cluster Failed!")
        
    config['DWH']['DWH_ENDPOINT'] = host
    print("-- Set  DWH_ENDPOINT to 'dwh.cfg' ...")
    with open('dwh.cfg', 'w') as configfile:    
        config.write(configfile)
               
    return host


def open_tcp_port():
    """ Open an incoming TCP port to access the cluster ednpoint
     
    Returns:
            No return values
    """
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
   
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

        
def create_s3_bucket():
    """ Create S3 bucket and upload dataset's
     
    Returns:
            No return values
    """
    
    # Create Bucket
    print("-- Create Bucket --> {}".format(BUCKET))
    s3.create_bucket(Bucket=BUCKET,)
    
    # Upload path_log.json
    print("-- Updaload log_json_path.json")
    s3.meta.client.upload_file(PATH_LOG_JSON, 
                               BUCKET, 
                               PATH_LOG_JSON)
    
    # Upload Log Dataset
    print("-- Upload Log Dataset")
    upload_dir(localDir=PATH_LOG_FROM, 
               bucketName=BUCKET,
               tag='*json',
               prefix='/')  
    
    # Upload Song Dataset
    print("-- Upload Song Dataset")
    upload_dir(localDir=PATH_SONG_FROM, 
               bucketName=BUCKET,
               tag='*json',
               prefix='/')    

  
def upload_dir(localDir, bucketName, tag, prefix):
    """
    from current working directory, upload a 'localDir' with all its subcontents (files and subdirectories...)
    to a aws bucket
    Parameters
    ----------
    localDir :   localDirectory to be uploaded, with respect to current working directory
    bucketName : bucket in aws
    tag :        tag to select files, like *png
                 NOTE: if you use tag it must be given like --tag '*txt', in some quotation marks... for argparse
    prefix :     to remove initial '/' from file names

    Returns
    -------
    None
    """
    
    cwd = str(Path.cwd())
    p = Path(os.path.join(Path.cwd(), localDir))
    mydirs = list(p.glob('**'))
    file_count = int(0)
  
    for mydir in mydirs:
        fileNames = glob.glob(os.path.join(mydir, tag))
        fileNames = [f for f in fileNames if not Path(f).is_dir()]
        rows = len(fileNames)
        for i, fileName in enumerate(fileNames):
            fileName = str(fileName).replace(cwd, '')
            if fileName.startswith(prefix):  
                fileName = fileName.replace(prefix, "", 1) 
            file_count = file_count + int(1)
            s3.meta.client.upload_file(fileName, bucketName, fileName)
            
    print("-- Total files uploaded --> {}".format(file_count))
        
        
if __name__=='__main__':   
    """
    - load DWH Configuration Params from a file 'dwh.cfg'    
    - Creates redshift client   
    - Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)   
    - Create Redshift Cluster   
    - Open an incoming TCP port to access the cluster ednpoint
    """
    
    # load DWH Params from a file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                     = config.get('AWS','KEY')
    SECRET                  = config.get('AWS','SECRET')
    IAM_ROLE_NAME           = config.get('IAM','IAM_ROLE_NAME')
    BUCKET                  = config.get('S3','BUCKET')
    PATH_LOG_FROM           = config.get('S3','PATH_LOG_FROM')
    PATH_SONG_FROM          = config.get('S3','PATH_SONG_FROM')
    PATH_LOG_JSON           = config.get('S3','PATH_LOG_JSON')
    DWH_NAME                = config.get('DWH', 'DWH_NAME')
    DWH_USER                = config.get('DWH', 'DWH_USER')
    DWH_PASSWORD            = config.get('DWH', 'DWH_PASSWORD')
    DWH_PORT                = config.get('DWH', 'DWH_PORT')
    DWH_CLUSTER_TYPE        = config.get('DWH','DWH_CLUSTER_TYPE')
    DWH_NODE_TYPE           = config.get('DWH','DWH_NODE_TYPE')
    DWH_NUM_CLUSTERS        = config.get('DWH','DWH_NUM_CLUSTERS')
    DWH_CLUSTER_IDENTIFIER  = config.get('DWH','DWH_CLUSTER_IDENTIFIER')
    
    # Create client for Redshift
    redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    # Create client for IAM
    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-east-1')

    # Create client for Cluster
    ec2 = boto3.resource('ec2',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    
    # Create client for S3
    s3 = boto3.resource('s3',region_name = 'us-east-1',
                     aws_access_key_id = KEY,
                     aws_secret_access_key = SECRET)    

    
    # Create an IAM Role that makes Redshift able to access S3 bucket (ReadOnly)
    IAM_ROLE = create_iam_role()

    # Create Redshift Cluster
    DWH_ENDPOINT = create_redshift_cluster()
       
    # Open an incoming TCP port to access the cluster ednpoint
    open_tcp_port()
    
    # Create S3 bucket and upload dataset's
    create_s3_bucket()