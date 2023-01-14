import argparse
import boto3
import logging

ssm_client = boto3.client('ssm')
sts_client = boto3.client('sts')

logger = logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

NOAA_RESPONSE_TYPES = ['forecast', 'forecastHourly', 'forecastGridData']

def get_parameters():
    params = {}
    params = {
        'opensky_username': ssm_client.get_parameter(Name='/opensky/username')['Parameter']['Value'],
        'opensky_password': ssm_client.get_parameter(Name='/opensky/password')['Parameter']['Value'],
        'bronze_bucket': ssm_client.get_parameter(Name='/aw-metar/bronze_bucket')['Parameter']['Value'],
        'silver_bucket': ssm_client.get_parameter(Name='/aw-metar/silver_bucket')['Parameter']['Value'],
        'work_bucket': ssm_client.get_parameter(Name='/aw-metar/work_bucket')['Parameter']['Value'],
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