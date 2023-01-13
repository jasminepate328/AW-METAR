import argparse
import boto3

ssm_client = boto3.client('ssm')
sts_client = boto3.client('sts')

def get_parameters():
    params = {}
    params = {
        'bronze_bucket': ssm_client.get_parameter(Name='/sk-anlys/bronze_bucket')['Parameter']['Value'],
        'silver_bucket': ssm_client.get_parameter(Name='/sk-anlys/silver_bucket')['Parameter']['Value'],
        'work_bucket': ssm_client.get_parameter(Name='/sk-anlys/work_bucket')['Parameter']['Value'],

    }    
    return params

def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description='Arguments required for script.')
    parser.add_argument('-b', '--bucket', required=False, choices=['work', 'bronze', 'prod'], help='Environment')
    parser.add_argument('-a', '--app_name', required=True, help='Environment')

    args = parser.parse_args()
    return args

def config():
    return {
        "region": boto3.DEFAULT_SESSION.region_name,
        "account_id": sts_client.get_caller_identity()['Account']
    }