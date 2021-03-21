from pyspark.sql import SparkSession
import pandas as pd
import random
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
import requests

spark = SparkSession \
    .builder \
    .appName('RealtimeKafkaML') \
    .master("local[3]") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

kafka_df = spark \
    .readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', "localhost:9092") \
    .option("startingOffsets", "earliest") \
    .option('subscribe', 'test') \
    .load()

schema = StructType([StructField('data', StringType())])

kafka_df.printSchema()

value_df = kafka_df.select(from_json(col("value").cast("string"),schema).alias("value"))

value_df.printSchema()

explode_df = value_df.selectExpr("value.data")

explode_df.printSchema()

explode_df = explode_df.select('*')

console_query = explode_df \
    .writeStream \
    .trigger(once=True) \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "chk-point-dir") \
    .start()

console_query.awaitTermination()
