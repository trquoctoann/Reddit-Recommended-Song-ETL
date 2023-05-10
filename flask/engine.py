import logging
from pyspark.sql import functions
from pyspark.sql.functions import col, desc
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.clustering import KMeans
from difflib import SequenceMatcher

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RecommendationEngine:
    
    def __init__(self, spark):
        logger.info("Starting up the Recommendation Engine: ")
        self.spark = spark
        self.spotify_data = self.__load_data_from_datalake()
        self.standardize_data, self.standardscaler, self.assemble = self.__vectorize_standardize_data()
        self.model, self.final_df = self.__train_model() 
    
    def __load_data_from_datalake(self) :
        logger.info("Loading Spotify data...")
        spotify_data = self.spark.read.parquet('/datalake/SpotifyData').drop('post_id', 'post_created_utc', 'post_title', 'post_url', \
                                                                             'key', 'mode', 'speechiness', 'acousticness', 'liveness', \
                                                                             'tempo', 'time_signature', 'year', 'month', 'day')
        def is_similar(str1, str2):
            matcher = SequenceMatcher(None, str1, str2)
            similarity = matcher.ratio()
            return similarity 

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
        get_genres_udf = functions.udf(get_genres, StringType())
        spotify_data = spotify_data.withColumn("major_genre", get_genres_udf(spotify_data["genres"]))
        return spotify_data
    
    def __vectorize_standardize_data(self):
        spotify_data = self.spotify_data
        spotify_data = spotify_data.na.drop()
        assemble = VectorAssembler(inputCols = ['danceability',
                                                'energy',
                                                'loudness',
                                                'instrumentalness',
                                                'valence'], outputCol='features')
        assembled_data = assemble.transform(spotify_data)
        
        scale = StandardScaler(inputCol = 'features',outputCol = 'standardized')
        standardscaler = scale.fit(assembled_data)
        standardize_data = standardscaler.transform(assembled_data)
        return standardize_data, standardscaler, assemble

    def __train_model(self):
        clusters = 5
        seed = 3
        logger.info("Training K-Means model...")
        
        KMeans_algo = KMeans(featuresCol = 'standardized', k = clusters, seed = seed)
        model = KMeans_algo.fit(self.standardize_data)
        final_df = model.transform(self.standardize_data)
        
        logger.info("K-Means model built!")
        return model, final_df
    
    def get_recommendation(self, danceability = 0.5, energy = 0.5, loudness = -10.1, instrumentalness = 0.5, valence = 0.5):
        input_ = [(danceability, energy, loudness, instrumentalness, valence)]
        schema = StructType([StructField("danceability", FloatType(), True)\
                            ,StructField("energy", FloatType(), True)\
                            ,StructField("loudness", FloatType(), True)\
                            ,StructField("instrumentalness", FloatType(), True)\
                            ,StructField("valence", FloatType(), True)])
        input_df = self.spark.createDataFrame(data = input_,schema = schema)
        
        assembled_input_data = self.assemble.transform(input_df)
        
        standardize_input_data = self.standardscaler.transform(assembled_input_data)
        predicted_cluster = self.model.transform(standardize_input_data).select('prediction').collect()[0][0]
        
        similar_cluster_songs = self.final_df.filter(col('prediction') == predicted_cluster).orderBy(desc('track_popularity'))
        recommendation = similar_cluster_songs.limit(5).collect()
        return recommendation