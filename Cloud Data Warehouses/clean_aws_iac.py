import boto3
import configparser
     
if __name__=='__main__':
    # load DWH Params from a file
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    DWH_CLUSTER_IDENTIFIER  = config.get('DWH','DWH_CLUSTER_IDENTIFIER')
    DWH_PORT = config.get('DWH','DWH_PORT')
    DWH_IAM_ROLE_NAME = config.get('IAM','IAM_ROLE_NAME')
    BUCKET = config.get('S3','BUCKET')
    
    # create redshift server
    redshift = boto3.client('redshift',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )
    
    # create EC2
    ec2 = boto3.resource('ec2',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    
    # Create client for IAM
    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-east-1')
    
    # Create client for S3
    s3 = boto3.resource('s3',region_name = 'us-east-1',
                     aws_access_key_id = KEY,
                     aws_secret_access_key = SECRET)
    
   
    # Delete Cluster
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get('DWH','DWH_CLUSTER_IDENTIFIER'))['Clusters'][0]
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        
        defaultSg.revoke_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)
    redshift.delete_cluster( ClusterIdentifier = DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get('DWH','DWH_CLUSTER_IDENTIFIER'))['Clusters'][0]
    print('-- {}'.format(myClusterProps['ClusterStatus']))
    
    # Delete Role
    print('-- Delete Role')
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    
    # Delete Bucket
    print("-- Delete Bucket")
    bucket = s3.Bucket(BUCKET)
    bucket.objects.all().delete()
    bucket.delete()
