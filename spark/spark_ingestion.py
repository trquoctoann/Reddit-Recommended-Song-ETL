# Import libraries
import os
import sys
import datetime
import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import *

path = os.getcwd()
runTime = datetime.datetime.today().strftime('%Y-%m-%d')
filename = datetime.datetime.today().strftime('%Y%m%d%H')
df = pd.read_csv(os.path.join(path, filename + '.csv'))

spark = SparkSession.builder \
    .appName("Ingestion HDFS") \
    .getOrCreate()

schema = StructType([StructField("post_id", StringType(), True)\
                    ,StructField("post_created_utc", StringType(), True)\
                    ,StructField("post_title", StringType(), True)
                    ,StructField("post_url", StringType(), True)\
                    ,StructField("artists_id", StringType(), True)\
                    ,StructField("artists_name", StringType(), True)\
                    ,StructField("track_id", StringType(), True)\
                    ,StructField("track_name", StringType(), True)\
                    ,StructField("release_date", StringType(), True)\
                    ,StructField("genres", StringType(), True)\
                    ,StructField("track_popularity", IntegerType(), True)\
                    ,StructField("danceability", FloatType(), True)\
                    ,StructField("energy", FloatType(), True)\
                    ,StructField("key", IntegerType(), True)\
                    ,StructField("mode", StringType(), True)\
                    ,StructField("loudness", FloatType(), True)\
                    ,StructField("speechiness", FloatType(), True)\
                    ,StructField("acousticness", FloatType(), True)\
                    ,StructField("instrumentalness", FloatType(), True)\
                    ,StructField("liveness", FloatType(), True)\
                    ,StructField("valence", FloatType(), True)\
                    ,StructField("tempo", FloatType(), True)\
                    ,StructField("duration_ms", IntegerType(), True)\
                    ,StructField("time_signature", IntegerType(), True)])

spark_df = spark.createDataFrame(df, schema = schema)
tblLocation = 'hdfs://namenode:9000/datalake/SpotifyData'

# Save to datalake
runTime = runTime.split('-')
year = runTime[0]
month = runTime[1]
day = runTime[2]
outputDF = spark_df.withColumn("year", functions.lit(year)).withColumn("month", functions.lit(month)) \
                 .withColumn("day", functions.lit(day))
outputDF.write.partitionBy("year", "month", "day").mode("append").parquet(tblLocation)
spark.stop()

if os.path.exists(os.path.join(path, filename + '.csv')):
    os.remove(os.path.join(path, filename + '.csv'))