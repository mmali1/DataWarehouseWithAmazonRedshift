import os
import pandas as pd
import boto3
import json
import configparser
import time

def parse_config(file):
    """
    Reads dwh.cfg file and load required config variables
    """
    config = configparser.ConfigParser()
    config.read_file(open(file))
    return config

    
def get_resource(resource_type,config):
    """
    Gets requested resource
    """
    resource = boto3.resource(resource_type,
                     region_name="us-west-2",
                     aws_access_key_id=config.get("AWS", "KEY"),
                     aws_secret_access_key=config.get("AWS","SECRET")
                    )
    return resource
  
def get_client(client_type,config):
    """
    Gets requested client
    """
    client = boto3.client(client_type,
                   aws_access_key_id=config.get("AWS", "KEY"),
                   aws_secret_access_key=config.get("AWS","SECRET"),
                   region_name="us-west-2")
    return client

def create_role(iam, config):
    """
    Creates IAM role
    """
    try:
        dwhRole = iam.create_role(
                Path='/',
                RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"),
                Description="Allows Redshift clusters to call AWS services on your behalf.",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [{'Action': 'sts:AssumeRole',
                     'Effect': 'Allow',
                     'Principal': {'Service': 'redshift.amazonaws.com'}}],
                     'Version': '2012-10-17'})
                )
    except iam.exceptions.EntityAlreadyExistsException:
        print('IAM role already exists.')
        dwhRole = iam.get_role(RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"))
        print('IAM Role Arn:', dwhRole['Role']['Arn'])

    return dwhRole

def attach_policy(iam,config ):
    """
    Attaches policy
    """
    try:
        iam.attach_role_policy(RoleName=config.get("DWH", "DWH_IAM_ROLE_NAME"),
                       PolicyArn=config.get("IAM_ROLE", "ARN_POLICY")
                      )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print(e)

def create_redshift_cluster(redshift,config,iam_role):
    """
    Creates redshift cluster
    """
    try:
        response = redshift.create_cluster(        
            # hardware
            ClusterType   = config.get("DWH","DWH_CLUSTER_TYPE"),
            NodeType      = config.get("DWH","DWH_NODE_TYPE"),
            NumberOfNodes = int(config.get("DWH","DWH_NUM_NODES")),

            #Identifiers & Credentials
            DBName             = config.get("DWH","DWH_DB"),
            ClusterIdentifier  = config.get("DWH","DWH_CLUSTER_IDENTIFIER"),
            MasterUsername     = config.get("DWH","DWH_DB_USER"),
            MasterUserPassword = config.get("DWH","DWH_DB_PASSWORD"),
        
            #Roles (for s3 access)
            IamRoles=[iam_role['Role']['Arn']]    
            )
        while True:
            cluster_status = redshift.describe_clusters(
                ClusterIdentifier=config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
            )['Clusters'][0]
            if cluster_status['ClusterStatus'] == "available":
                break
            print('Cluster Status', cluster_status['ClusterStatus'])
            time.sleep(10)
    
        print("Cluster is created and available.")      
    
    except Exception as e:
        print(e)

    clusterProps = redshift.describe_clusters(
        ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER")
        )['Clusters'][0]
    return clusterProps
              
def main():
    """
    Get the config
    Print the config
    Get the resources (ec2 and s3)
    """
    print("Reading config file...")
    config_file_name = './dwh.cfg'
    config = parse_config(config_file_name)
    
    #print("Config parameters are")
    #print_config_info(config)
    
    print("Getting ec2 resource...")
    ec2 = get_resource('ec2',config)
    
    print("Getting s3 resource...")
    s3 = get_resource('s3',config)
    
    print("Getting iam client to create iamrole...")
    iam = get_client('iam',config)
    
    print("Getting redshift client to create cluster...")
    redshift = get_client('redshift',config)
    
    print("Creating new IAM role...")
    iam_role = create_role(iam, config)
    
    print("Attaching s3 read only access policy...")
    attach_policy(iam, config)
    
    print("Creating redshift cluster...")
    myClusterProps=create_redshift_cluster(redshift,config,iam_role)

    print("DWH_ENDPOINT :: ", myClusterProps['Endpoint']['Address'])
    print("DWH_ROLE_ARN :: ", myClusterProps['IamRoles'][0]['IamRoleArn'])
    
if __name__ == "__main__":
    main()