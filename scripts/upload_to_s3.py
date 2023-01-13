import logging
import os
import boto3
from botocore.exceptions import ClientError
from utils.helper_functions import *

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')

def main():
    args = parse_args()
    params = get_parameters(args)

    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

    if dir_path.endswith('pyspark_apps'):
        path = f'{dir_path}/pyspark_apps/'
        bucket_name = params['work_bucket']
    else:
        path = f'{dir_path}/raw_data/'
        bucket_name =params['bronze_bucket']

    upload_directory(path, bucket_name)

def upload_directory(path, bucket_name):
    for root, _, files in os.walk(path):
        for file in files:
            try:
                if file != '.DS_Store':
                    file_directory = os.path.basename(os.path.dirname(os.path.join(root, file)))
                    key = f'{file_directory}/{file}'
                    s3_client.upload_file(os.path.join(root, file), bucket_name, key)
                    print(f"File '{key} upload to bucket '{bucket_name}' as key '{key}'")
            except ClientError as e:
                logging.error(e)


if __name__ == '__main__':
    main()