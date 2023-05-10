# Import libraries
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from difflib import SequenceMatcher

# function to validate veracity of searched song
def is_similar(str1, str2):
    matcher = SequenceMatcher(None, str1, str2)
    similarity = matcher.ratio()
    return similarity 

#get major genre from data
def get_genres(text):
    if text is None:
        return None
    genres = {'country': 0, 'electronic': 0, 'funk': 0, 'hip hop': 0, 'jazz': 0, 'rap': 0, 'classical': 0, 'dance': 0, 'soul': 0, 
              'indie': 0, 'latin': 0, 'pop': 0, 'punk': 0, 'reggae': 0, 'rock': 0, 'metal': 0, 'r&b': 0, 'house': 0, 'techno': 0, 'folk': 0}
    for each in text.split(', '):
        each = each.replace(' ', '')
        for genre in genres:
            genres[genre] += is_similar(genre, each)
    sorted_genres = dict(sorted(genres.items(), key=lambda item: item[1], reverse = True))
    return next(iter(sorted_genres))

# create SparkSession
spark = SparkSession.builder.appName("Daily Genre Report") \
    .config("hive.metastore.uris", "thrift://metastore-server:9083") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .enableHiveSupport() \
    .getOrCreate()

runTime = '2023-05-07'
# runTime = datetime.datetime.today().strftime('%Y-%m-%d')
runTime = runTime.split('-')
year = runTime[0]
month = runTime[1]
day = runTime[2]

# load data to Spark DataFrames
spotifyDF = spark.read.parquet("/datalake/SpotifyData").drop("year", "month", "day")

get_genres_udf = functions.udf(get_genres, StringType())
spotifyDF = spotifyDF.withColumn("major_genre", get_genres_udf(spotifyDF["genres"]))

sqlDF = spotifyDF.createOrReplaceTempView("spotify")

# aggregate data
mapDF = spark.sql("""
    SELECT major_genre AS Genre,
           COUNT(*) AS NumberOfSongs,
           FLOOR(AVG(track_popularity)) AS Popularity,
           ROUND(AVG(danceability), 3) AS Danceability,
           ROUND(AVG(energy), 3) AS Energy,
           ROUND(AVG(valence), 3) AS Valence,
           ROUND(AVG(loudness), 3) AS Loudness
    FROM spotify
    GROUP BY major_genre
""")

# prepare result
resultDF = mapDF.withColumn("year", lit(year)) \
                .withColumn("month", lit(month)) \
                .withColumn("day", lit(day)) \
                .select("Genre", "NumberOfSongs", "Popularity", "Danceability", "Energy", \
                        "Valence", "Loudness", "year", "month", "day")

# write to data warehouse
resultDF.write.format("hive") \
    .partitionBy("year", "month", "day") \
    .mode("append") \
    .saveAsTable("default.daily_genre_report")