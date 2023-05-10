import requests
import configparser

configFilePath = r'project.conf'

# create the authentication header to query spotify
def get_spotify_bearer_token():
    # get spotify api info
    parser = configparser.ConfigParser()
    parser.read(configFilePath)
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