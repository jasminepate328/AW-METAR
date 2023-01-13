import os

import boto3
from pyspark.sql import SparkSession
from utils.helper_functions import *


s3_client = boto3.client('s3')

def main():
    params = get_parameters()
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    
    spark = SparkSession.builder.appName("csv-to-parquet").getOrCreate()
    for file in s3_client.list_objects(Bucket=params['bronze_bucket'], Delimiter='/'):
        convert_to_parquet(spark, dir_path + file, params)

def convert_to_parquet(spark, file_path, params):
    sky_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .load(f"s3a://{params['bronze_bucket']}/{file_path}.csv")

    sky_df.write \
        .format("parquet") \
        .save(f"s3a://{params['silver_bucket']}/{file_path}/", mode="overwrite")

if __name__ == "__main__":
    main()