import time

from pyspark.sql.types import StructField, StringType, IntegerType, StructType, FloatType, TimestampType
from pyspark.sql.window import Window
import pyspark.sql.functions as F

def create_window():
    schema = StructType([
        StructField("placeholder1", IntegerType(), False),
        StructField("placeholder2", TimestampType(), False),
        StructField("placeholder3", FloatType(), False),
        StructField("placeholder4", StringType(), False)
    ])

    window = Window.partitionBy("placeholder4").orderBy("placeholder3")
    window_agg = Window.partitionBy("placeholder4")

    return schema, window, window_agg

def console(df, args):
    schema, window, window_agg = create_window()

    df \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", schema=schema).alias("data")) \
        .select("data.*") \
        .withColumn("row", F.row_number().over(window)) \
        .withColumn("air1", F.count(F.col("placeholder3")).over(window_agg)) \
        .withColumn("air2", F.sum(F.col("placeholder3")).over(window_agg)) \
        .filter(F.col("row") == 1).drop("row") \
        .select("placeholder4",
                F.format_number("air2", 2).alias("air2"),
                F.format_number("air1", 0).alias("air1")) \
        .coalesce(1) \
        .orderBy(F.regexp_replace("air2", ",", "").cast("float"), ascending=False) \
        .write \
        .format("console") \
        .option("numRows", 25) \
        .option("truncate", False) \
        .save()


def cvs_s3(df, args):
    schema, window, window_agg = create_window()

    df \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", schema=schema).alias("data")) \
        .select("data.*") \
        .withColumn("row", F.row_number().over(window)) \
        .withColumn("air1", F.count(F.col("placeholder3")).over(window_agg)) \
        .withColumn("air2", F.sum(F.col("placeholder3")).over(window_agg)) \
        .filter(F.col("row") == 1).drop("row") \
        .select("placeholder4",
                F.format_number("air2", 2).alias("air2"),
                F.format_number("air1", 0).alias("air1")) \
        .coalesce(1) \
        .orderBy(F.regexp_replace("air2", ",", "").cast("float"), ascending=False) \
        .write \
        .csv(path=f"s3a://{args.s3_bucket}/output/", header=True, sep="|")

def stream_to_kafka(df, options_write):
    schema, window, window_agg = create_window()

    df \
        .selectExpr("CAST(value AS STRING)") \
        .select(F.from_json("value", schema=schema).alias("data")) \
        .select("data.*") \
        .withColumn("row", F.row_number().over(window)) \
        .withColumn("air1", F.count(F.col("placeholder3")).over(window_agg)) \
        .withColumn("air2", F.sum(F.col("placeholder3")).over(window_agg)) \
        .filter(F.col("row") == 1).drop("row") \
        .select("placeholder4",
                F.format_number("air2", 2).alias("air2"),
                F.format_number("air1", 0).alias("air1")) \
        .coalesce(1) \
        .orderBy(F.regexp_replace("air2", ",", "").cast("float"), ascending=False) \
        .select(F.to_json(F.struct("*"))).toDF("value") \
        .write \
        .format("kafka") \
        .options(**options_write) \
        .save()

def stream_from_kafka(df):
    schema = create_window()[0]
    ds_air2 = df \
        .selectExpr("CAST(value AS STRING)", "timestamp") \
        .select(F.from_json("value", schema=schema).alias("data"), "timestamp") \
        .select("data.*", "timestamp") \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy("placeholder4",
                 F.window("timestamp", "10 minutes", "5 minutes")) \
        .agg(F.sum("placeholder3"), F.count("placeholder3")) \
        .orderBy(F.col("window").desc(),
                 F.col("sum(placeholder3)").desc()) \
        .select("placeholder4",
                F.format_number("sum(placeholder3)", 2).alias("air2"),
                F.format_number("count(placeholder3)", 0).alias("air1"),
                "window.start", "window.end") \
        .coalesce(1) \
        .writeStream \
        .queryName("streaming_to_console") \
        .trigger(processingTime="1 minute") \
        .outputMode("complete") \
        .format("console") \
        .option("numRows", 10) \
        .option("truncate", False) \
        .start()

    ds_air2.awaitTermination()


def read_from_csv(spark, args):
    schema = create_window()[0]
    df = spark.read \
        .csv(path=f"s3a://{args.s3_bucket}/raw_data/{args.raw_data_file}",
             schema=schema, header=True, sep="|")

    return df

def write_to_kafka(spark, df, options_write, args):
    air2_count = df.count()

    for r in range(0, air2_count):
        row = df.collect()[r]
        df_message = spark.createDataFrame([row], df.schema)

        df_message = df_message \
            .drop("placeholder2") \
            .withColumn("placeholder2", F.current_timestamp())

        df_message \
            .selectExpr("CAST(placeholder1 AS STRING) AS key",
                        "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .options(**options_write) \
            .save()

        df_message.show(1)
        time.sleep(args.message_delay)
