import argparse
import boto3
import logging
from botocore.exceptions import ClientError

ssm_client = boto3.client('ssm')
sts_client = boto3.client('sts')

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

NOAA_RESPONSE_TYPES = ['forecast', 'forecastHourly', 'forecastGridData']

def get_parameters(requested_params) -> list:
    try:
        params = {
            'opensky_username': '/opensky/username',
            'opensky_password': '/opensky/password',
            'bronze_bucket': '/aw-metar/bronze_bucket',
            'silver_bucket': '/aw-metar/silver_bucket',
            'work_bucket': '/aw-metar/work_bucket'
        }
        return {x: ssm_client.get_parameter(Name=params[x])['Parameter']['Value'] for x in requested_params} 
    except ClientError as e:
        logging.error(e)   

def parse_args():
    """Parse argument values from command-line"""

    parser = argparse.ArgumentParser(description='Arguments required for script.')
    parser.add_argument('-b', '--bucket', required=False, choices=['work', 'bronze', 'prod'], help='Environment')
    parser.add_argument('-a', '--app_name', required=False, help='Environment')
    parser.add_argument('-t', '--job-type', required=False, choices=['process', 'analyze'], help='process or analysis')
    parser.add_argument('-c', '--crawler-name', required=False, help='Name of EC2 Keypair')

    args = parser.parse_args()
    return args

def config():
    return {
        "region": boto3.DEFAULT_SESSION.region_name,
        "account_id": sts_client.get_caller_identity()['Account']
    }
