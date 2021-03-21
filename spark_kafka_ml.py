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
schema_output = StructType([StructField('neg', StringType()),\
                            StructField('pos', StringType()),\
                            StructField('neu', StringType()),\
                            StructField('compound', StringType())])
kafka_df.printSchema()
#Function to send post request to flask application
def apply_sentiment_analysis(data):
    import requests
    import json

    result = requests.post('http://localhost:5000/predict', json=json.loads(data))
    return json.dumps(result.json())

vader_udf = udf(lambda data: apply_sentiment_analysis(data), StringType())

value_df = kafka_df.select(from_json(col("value").cast("string"),schema).alias("sentence"),\
                           from_json(vader_udf(col("value").cast("string")),schema_output).alias("response"))
value_df.printSchema()

explode_df = value_df.select("sentence.data", "response.*")

console_query = explode_df \
    .writeStream \
    .trigger(processingTime="1 minute") \
    .format("console") \
    .outputMode("append") \
    .option("checkpointLocation", "chk-point-dir") \
    .start()

console_query.awaitTermination()
############Test kafka producer if spark reads kafka message correctly###########
# value_df = kafka_df.select(from_json(col("value").cast("string"),schema).alias("value"))
#
# value_df.printSchema()
#
# explode_df = value_df.selectExpr("value.data")
#
# explode_df.printSchema()
#
# explode_df = explode_df.select('*')
#
# console_query = explode_df \
#     .writeStream \
#     .trigger(once=True) \
#     .format("console") \
#     .outputMode("append") \
#     .option("checkpointLocation", "chk-point-dir") \
#     .start()
#
# console_query.awaitTermination()
