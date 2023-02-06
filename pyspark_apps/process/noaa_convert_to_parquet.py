import os

import boto3
from pyspark.sql import SparkSession

from utils.helper_functions import *

s3_client = boto3.client('s3')

def main():
    params = get_parameters()
    dir_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    
    spark = SparkSession.builder.appName("noaa-convert-to-parquet").getOrCreate()
    convert_to_parquet(spark, dir_path + f"noaa.{format}", params)

def convert_to_parquet(spark, file_path, format, params):
    noaa_df = spark.read \
        .format(format) \
        .option("header", "true") \
        .option("delimiter", ",") \
        .option("inferSchema", "true") \
        .load(f"s3://{params['bronze_bucket']}/{file_path}.{format}")

    noaa_df.write \
        .format("parquet") \
        .save(f"s3://{params['silver_bucket']}/{file_path}/", mode="overwrite")

if __name__ == "__main__":
    main()