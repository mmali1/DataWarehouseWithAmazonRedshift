import os
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
    Gets the requested resource
    """
    resource = boto3.resource(resource_type,
                     region_name="us-west-2",
                     aws_access_key_id=config.get("AWS", "KEY"),
                     aws_secret_access_key=config.get("AWS","SECRET")
                    )
    return resource
  
def get_client(client_type,config):
    """
    Gets the requested client
    """
    client = boto3.client(client_type,
                   aws_access_key_id=config.get("AWS", "KEY"),
                   aws_secret_access_key=config.get("AWS","SECRET"),
                   region_name="us-west-2")
    return client

def delete_iam(iam,config):
    """
    Deletes IAM role
    """
    try:
        iam.detach_role_policy(
                RoleName=config.get("DWH","DWH_IAM_ROLE_NAME"),
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
        )
        iam.delete_role(RoleName=config.get("DWH","DWH_IAM_ROLE_NAME"))
    except Exception as e:
        print(e)
        
def delete_cluster(redshift,config):
    """
    Deletes redshift cluster
    """
    try:
        redshift.delete_cluster( 
            ClusterIdentifier = config.get("DWH","DWH_CLUSTER_IDENTIFIER"),
            SkipFinalClusterSnapshot=True
        )
        
        while True:
            try:
                cluster_status = redshift.describe_clusters(
                    ClusterIdentifier=config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
                )['Clusters'][0]
            except Exception as e:
                print("Cluster is not available")
                cluster_status = None
                
            if cluster_status is None:
                print("Cluster is deleted")
                break
            print('Cluster Status', cluster_status['ClusterStatus'])
            time.sleep(10)
                 
    except Exception as e:
        print(e)
        
        
def main():
    """
    Get the config
    Delete IAM role
    Delete Cluster
    """
    print("Reading config file")
    config_file_name = './dwh.cfg'
    config = parse_config(config_file_name)    

    iam = get_client('iam',config)
    print("Deleting IAM role...")
    delete_iam(iam,config)
    
    redshift = get_client('redshift', config)
    print("Deleting redshift cluster...")
    delete_cluster(redshift,config)
    
if __name__ == "__main__":
    main()