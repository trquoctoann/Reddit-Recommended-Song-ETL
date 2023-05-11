import flask
import os
import requests
import random

from engine import RecommendationEngine
from pyspark.sql import SparkSession
from utils import (
    get_spotify_bearer_token
)

header = get_spotify_bearer_token()
path = os.getcwd()

def translate_danceability(danceability): 
    if danceability == 'Not Danceable':
        return random.uniform(0, 0.3)
    elif danceability == 'Rhythmic':
        return random.uniform(0.3, 0.6)
    elif danceability == 'Perfect For Dancing':
        return random.uniform(0.6, 1)
    elif danceability == '':
        return 0.5
    
def translate_energy(energy): 
    if energy == 'Calm':
        return random.uniform(0, 0.3)
    elif energy == 'Quite Exciting':
        return random.uniform(0.3, 0.6)
    elif energy == 'Extremely Exciting':
        return random.uniform(0.6, 1)
    elif energy == '':
        return 0.5

def translate_loudness(loudness):
    if loudness == 'Soft':
        return random.uniform(-5, 0)
    elif loudness == 'Average Loudness':
        return random.uniform(-10, -5)
    elif loudness == 'Very Loud':
        return random.uniform(-20, -10)
    elif loudness == '':
        return 0.5
    
def translate_instrumentalness(instrumentalness): 
    if instrumentalness == 'No':
        return random.uniform(0, 0.5)
    elif instrumentalness == 'Yes':
        return random.uniform(0.5, 1)
    elif instrumentalness == '':
        return 0.5

def translate_valence(valence): 
    if valence == 'Highly Negative':
        return random.uniform(0, 0.2)
    elif valence == 'Negative':
        return random.uniform(0.2, 0.4)
    elif valence == 'Balanced':
        return random.uniform(0.4, 0.6)
    elif valence == 'Positive':
        return random.uniform(0.6, 0.8)
    elif valence == 'Highly Positive':
        return random.uniform(0.8, 1)
    elif valence == '':
        return 0.5

spark = SparkSession.builder \
           .appName('Web App') \
           .getOrCreate()
engine = RecommendationEngine(spark)

danceability_option = ['Perfect For Dancing', 'Rhythmic', 'Not Danceable']
energy_option = ['Extremely Exciting', 'Quite Exciting', 'Calm']
instrumentalness_option = ['Yes', 'No']
loudness_option = ['Very Loud', 'Average Loudness', 'Soft']
valence_option = ['Highly Positive', 'Positive', 'Balanced', 'Negative', 'Highly Negative']

app = flask.Flask(__name__, template_folder = os.path.join(path, 'template'))
@app.route('/', methods = ['GET', 'POST'])
def main():
    if flask.request.method == 'GET':
        return flask.render_template('index.html', danceability_option = danceability_option, energy_option = energy_option, \
                                     loudness_option = loudness_option, instrumentalness_option = instrumentalness_option, \
                                     valence_option = valence_option)
            
    if flask.request.method == 'POST':
        danceability = translate_danceability(flask.request.form['danceability_option'])
        energy = translate_energy(flask.request.form['energy_option'])
        loudness = translate_loudness(flask.request.form['loudness_option'])
        instrumentalness = translate_instrumentalness(flask.request.form['instrumentalness_option'])
        valence = translate_valence(flask.request.form['valence_option'])
        results = engine.get_recommendation(danceability, energy, loudness, instrumentalness, valence)
        
        recommendations = []
        for result in results: 
            recommendation = []
            artist_name = result[1]
            track_id = result[2]
            track_name = result[3]
            release_date = result[4]
            track_popularity = result[6]
            duration = int(result[12]) / 1000
            genre = result[13]
            response = requests.get("https://api.spotify.com/v1/tracks/", headers = header, params = {'ids': track_id})
            link_cover_image = response.json()['tracks'][0]['album']['images'][0]['url']
            song_url = 'https://open.spotify.com/track/' + track_id

            recommendation.append(track_name)
            recommendation.append(artist_name)
            recommendation.append(release_date)
            recommendation.append(genre)
            recommendation.append(duration)
            recommendation.append(track_popularity)
            recommendation.append(link_cover_image)
            recommendation.append(song_url)
            recommendations.append(recommendation)
        return flask.render_template('result.html', recommendations = recommendations)

if __name__ == '__main__':
    app.run(host = "0.0.0.0")