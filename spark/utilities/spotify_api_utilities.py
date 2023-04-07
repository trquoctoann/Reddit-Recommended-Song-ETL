import requests
import configparser
import urllib3
from difflib import SequenceMatcher

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# create the authentication header to query spotify
def get_spotify_bearer_token():
    # get spotify api info
    parser = configparser.ConfigParser()
    parser.read("project.conf")
    auth_url = parser.get("spotify_api_config", "spotify_auth_url")
    client_id = parser.get("spotify_api_config", "spotify_api_client_id")
    client_secret = parser.get("spotify_api_config", "spotify_api_client_secret")

    # connect to API
    payload = {"client_id": client_id, "client_secret": client_secret, "grant_type": "client_credentials"}
    response = requests.post(auth_url, data = payload)
    if response.status_code != 200:
        print("Error Response Code: " + str(response.status_code))
        raise Exception(response.status_code, response.text)
    access_token = response.json()["access_token"]
    header = {"Authorization": "Bearer " + access_token}
    return header

# create the url to search for item
def url_spotify_search_item(keyword, result_type = "track", limit = 1):
    endpoint_url = "https://api.spotify.com/v1/search"
    query_parameters = {'query': keyword, 'type': result_type, 'limit': limit}            
    return (endpoint_url, query_parameters)

# create the url to get tracks' audio features
def url_spotify_get_tracks_audio_features(keyword):
    endpoint_url = "https://api.spotify.com/v1/audio-features"
    query_parameters = {'ids': keyword}            
    return (endpoint_url, query_parameters)

# create the url to get artists
def url_spotify_get_several_artists(ids):
    endpoint_url = "https://api.spotify.com/v1/artists"
    query_parameters = {'ids': ids}            
    return (endpoint_url, query_parameters)

# function to validate veracity of searched song
def is_similar(str1, str2):
    matcher = SequenceMatcher(None, str1, str2)
    similarity = matcher.ratio()
    return similarity 

# initial connection
def send_spotify_get_request(url, headers, params):
    response = requests.get(url, headers = headers, params = params)
    if response.status_code != 200:
        print("Error Response Code: " + str(response.status_code))
        raise Exception(response.status_code, response.text)
    return response.json(), response.status_code