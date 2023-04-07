import requests
import configparser
import urllib3
import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# create the authentication header to query 
def get_reddit_bearer_token():
    # get spotify api info
    parser = configparser.ConfigParser()
    parser.read("project.conf")
    auth_url = parser.get("reddit_api_config", "reddit_auth_url")
    username = parser.get("reddit_api_config", "reddit_api_username")
    password = parser.get("reddit_api_config", "reddit_api_password")
    app_id = parser.get("reddit_api_config", "reddit_api_app_id")
    app_secret = parser.get("reddit_api_config", "reddit_api_app_secret")
    user_agent = parser.get("reddit_api_config", "reddit_api_user_agent")

    # connect to API
    payload = {'username': username, 'password': password, 'grant_type': 'password'}
    auth = requests.auth.HTTPBasicAuth(app_id, app_secret)
    response = requests.post(auth_url, data = payload, headers={'user-agent': user_agent}, auth = auth)
    if response.status_code != 200:
        print("Error Response Code: " + str(response.status_code))
        raise Exception(response.status_code, response.text)
    access_token = response.json()["access_token"]
    return "Bearer " + access_token

# convert created_utc value to datetime
def format_reddit_created_date(date):
    date_format = '%Y-%m-%d %H:%M:%S'
    dt = datetime.datetime.fromtimestamp(date)
    formatted_date = dt.strftime(date_format)
    return formatted_date

# send request to get new post
def send_request_reddit_get_new_post(url, access_token = get_reddit_bearer_token()):
    parser = configparser.ConfigParser()
    parser.read("project.conf")
    user_agent = parser.get("reddit_api_config", "reddit_api_user_agent")
    parameters = {'limit' : 100}
    response = requests.get(url, headers = {'Authorization' : access_token, 'user-agent': user_agent}, params = parameters)
    if response.status_code != 200:
        print("Error Response Code: " + str(response.status_code))
        raise Exception(response.status_code, response.text)
    return response.json(), response.status_code