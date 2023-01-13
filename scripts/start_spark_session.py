import argparse
from pyspark.sql import SparkSession
import pyspark_apps.analyze.pyspark_sql_functions as pyspark_sql_functions

CONFIG =  {
        "startingOffsets":
            "earliest",
        "endingOffsets":
            "latest",
        "kafka.security.protocol":
            "SASL_SSL",
        "kafka.sasl.mechanism":
            "AWS_MSK_IAM",
        "kafka.sasl.jaas.config":
            "software.amazon.msk.auth.iam.IAMLoginModule required;",
        "kafka.sasl.client.callback.handler.class":
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
    }

class Kafka:
    def __init__(self):

        self.args = self.parse_args()

        self.spark = SparkSession.builder.appName(self.args.app_name).getOrCreate()
        self.spark.sparkContext.setLogLevel("DEBUG")

    def read_from_kafka(self):
        options_read = {
            "kafka.bootstrap.servers":
                self.args.bootstrap_servers,
            "subscribe":
                self.args.read_topic,
            **CONFIG
        }

        df = self.spark.read.format("kafka").options(**options_read).load()
        run_script = getattr(pyspark_sql_functions, self.args.app_name) 
        run_script(df, self.args)
        return df

    def stream_to_kafka(self, df):
        options_write = {
            "kafka.bootstrap.servers":
                self.args.bootstrap_servers,
            "topic":
                self.args.write_topic,
            **CONFIG
        }
        df = pyspark_sql_functions.read_from_csv(self.spark, self.args)
        df.cache()

        pyspark_sql_functions.write_to_kafka(self.spark, df, options_write, self.args)

    def stream_from_kafka(self):
        options_read = {
            "kafka.bootstrap.servers":
                self.args.bootstrap_servers,
            "subscribe":
                self.args.read_topic,
            **CONFIG
        }
        df = self.spark.readStream.format("kafka").options(**options_read).load()
        pyspark_sql_functions.stream_from_kafka(df)
        
    def parse_args(self):
        """Parse argument values from command-line"""

        parser = argparse.ArgumentParser(description="Arguments required for script.")
        parser.add_argument("--bootstrap_servers", required=True, help="Kafka bootstrap servers")
        parser.add_argument("--read_topic", default="topicA", required=False, help="Kafka topic to read from")
        parser.add_argument("--s3_bucket", required=True, help="Amazon S3 bucket")
        parser.add_argument("--write_topic", default="topicB", required=False, help="Kafka topic to write to")
        parser.add_argument("--app_name", required=True, help="Script to run")
        parser.add_argument("--raw_data_file", default="_.csv", required=False, help="data file")
        parser.add_argument("--message_delay", default=0.5, required=False, help="message publishing delay")

        args = parser.parse_args()
        return args

if __name__ == "__main__":
    Kafka()