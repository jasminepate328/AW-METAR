import os

import boto3
from pyspark.sql import SparkSession
from utils.helper_functions import *
from utils.flatten_json import flatten

s3_client = boto3.client('s3')

def main():
    params = get_parameters(['bronze_bucket', 'silver_bucket'])
    dir_path = os.path.dirname(os.path.realpath('data/raw'))
    
    spark = SparkSession.builder.appName("opensky-convert-to-parquet").getOrCreate()
    convert_to_parquet(spark, dir_path + f"/opensky.json", params)

def convert_to_parquet(spark, file_path, params):
    sky_df = spark.read \
        .option("multiline", "true") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .json(f"s3a://{params['bronze_bucket']}/raw/{file_path}.json")
    flattened_sky_df = flatten(sky_df)

    flattened_sky_df.write \
        .format("parquet") \
        .save(f"s3a://{params['silver_bucket']}/processed/{file_path}/", mode="overwrite")

if __name__ == "__main__":
    main()