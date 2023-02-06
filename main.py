import os
import json

import boto3
from botocore.exceptions import ClientError

from data.imports.noaa import WeatherService
from data.imports.opensky import OpenSky
from utils.helper_functions import *

s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')

"""
 TODO: 
    Import data
    Get latitude and longitude from OpenSky, use values to pull weather for each location.
    Flatten JSON
    Convert to parquet and save file to S3 
"""

def import_data():
    # Get Aviation Data
    with open('data/raw/opensky.json', 'w') as outfile:
        os_states = OpenSky().get_states()
        outfile.write(os_states) 
    outfile.close()

    # Get Weather Data
    with open('data/raw/noaa.json', 'w') as outfile:
        lat_long = json.loads(os_states)
        for i in lat_long:
            forecast = WeatherService().data_by_coordinates(i['latitude'], i['longitude'], type="forecastGridData")
            if forecast:
                outfile.write(forecast)
    outfile.close()
    upload_directory()

def upload_directory():
    params = get_parameters(['bronze_bucket'])

    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    path = f'{dir_path}/AW-METAR/data/raw/'
    bronze_bucket = params['bronze_bucket']
    for root, _, files in os.walk(path):
        for file in files:
            try:
                if file != '.DS_Store':
                    file_directory = os.path.basename(os.path.dirname(os.path.join(root, file)))
                    key = f'{file_directory}/{file}'
                    s3_client.upload_file(os.path.join(root, file), bronze_bucket, key)
                    logging.info(f"File '{key} upload to bucket {bronze_bucket} as key '{key}'")
            except ClientError as e:
                logging.error(e)    

if __name__ == "__main__":
    import_data()