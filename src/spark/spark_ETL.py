# import libraries
import os
import re
import sys
import pandas as pd
import html
import datetime
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import StringType

#import functions from utilities folder
from utilities.reddit_api_utilities import (
    format_reddit_created_date,
    send_request_reddit_get_new_post,
)

from utilities.spotify_api_utilities import (
    get_spotify_bearer_token,
    url_spotify_search_item,
    url_spotify_get_tracks_audio_features,
    url_spotify_get_several_artists,
    is_similar,
    send_spotify_get_request
)

# access history file to get the last execution time
history_file = os.path.join(os.getcwd(), 'src/spark/data/History.csv')

if os.path.exists(history_file):
    with open(history_file, mode='r', encoding='utf-8') as f:
        history = f.readlines()[-1].split(',')
        rMusic_last_time = history[0]
        rIndieHeads_last_time = history[1]
        rPopHeads_last_time = history[2]
        rElectronicMusic_last_time = history[3][:-2]

# convert data type to datetime
rMusic_last_time = datetime.datetime.strptime(rMusic_last_time, '%Y-%m-%d %H:%M:%S')
rIndieHeads_last_time = datetime.datetime.strptime(rIndieHeads_last_time, '%Y-%m-%d %H:%M:%S')
rPopHeads_last_time = datetime.datetime.strptime(rPopHeads_last_time, '%Y-%m-%d %H:%M:%S')
rElectronicMusic_last_time = datetime.datetime.strptime(rElectronicMusic_last_time, '%Y-%m-%d %H:%M:%S')

##                                           REDDIT API - EXTRACTION & TRANSFORMATION 

# create historic df
historyDF = pd.DataFrame(columns = ['rMusic', 'rIndieHeads', 'rPopHeads', 'rElectronicMusic'])

# create dataframe to store subreddit post
redditDF = pd.DataFrame(columns = ['post_id', 'post_title', 'post_created_utc', 'post_url', 'searching_context'])
validation = {}

##                                                     EXTRACT FROM r/Music

# get reddit api data in json format
reddit_rMusic_response, reddit_rMusic_status_code = send_request_reddit_get_new_post('https://oauth.reddit.com/r/Music/new')

if reddit_rMusic_status_code != 200 : 
    sys.exit()

success = False
rMusic_execution_time = ''
for post in reddit_rMusic_response['data']['children'] :
    # only get posts which is after the last execution
    if datetime.datetime.strptime(format_reddit_created_date(post['data']['created_utc']), '%Y-%m-%d %H:%M:%S')  > rMusic_last_time :
        success = True
        # eliminate 'i made this' tag cause most of this kind of song can't find on spotify
        if post['data']['link_flair_text'] != 'i made this' :
          # recommendation posts usually have youtube, spotify, soundcloud, bandcamp link attached
            if 'youtu' in post['data']['url'] or 'spotify' in post['data']['url'] \
            or 'soundcloud' in post['data']['url'] or 'bandcamp' in post['data']['url'] :
                # get raw post title
                post_title_raw = post['data']['title']
                post_title_raw = html.unescape(post_title_raw)
                # ' - ' was used to separate songs name and artists name
                # only get posts have this character because of later validation
                if ' - ' in post_title_raw :
                    # get id, time and url
                    post_id = post['data']['id']
                    post_created_utc = format_reddit_created_date(post['data']['created_utc'])
                    post_url = 'https://www.reddit.com/r/Music/comments/' + post_id

                    # clean title text for searching purpose
                    # remove '(', '[' and it's following content
                    post_title = post_title_raw

                    start_searching_position = post_title.find(' - ')
                    removed_positions = [post_title.find('('), post_title.find('[')]
                    removed_positions.sort(reverse = True)

                    for removed_position in removed_positions : 
                        if removed_position > start_searching_position :
                            post_title = post_title[:removed_position]

                    # extract songs name and artist name for later validation
                    song_artist_name = post_title.split(' - ')
                    validation[post_title_raw] = [song_artist_name[0].strip(), song_artist_name[1].strip()]
                    
                    # remove strange symbols exclude spaces
                    post_title = re.sub('[\W_]+', ' ', post_title, flags = re.UNICODE)

                    # build new row to be added
                    redditDF_aux = pd.DataFrame({'post_id': [post_id], 'post_title': [post_title_raw], 
                                                'post_created_utc': [post_created_utc], 'post_url': [post_url], 
                                                'searching_context': [post_title]})
                    
                    # add new row to df
                    redditDF = pd.concat([redditDF_aux, redditDF], ignore_index = True, axis = 0)

                    if rMusic_execution_time == '' : 
                        rMusic_execution_time = post_created_utc
    else : 
        break

# add new execution time to historic dataframe
if rMusic_execution_time == '' :
    rMusic_execution_time = rMusic_last_time

##                                                  EXTRACT FROM r/IndieHeads

# get reddit api data in json format
reddit_rIndieHeads_response, reddit_rIndieHeads_status_code = send_request_reddit_get_new_post('https://oauth.reddit.com/r/indieheads/new/')

if reddit_rIndieHeads_status_code != 200 : 
    sys.exit()

count = 1
success = False
rIndieHeads_execution_time = ''
for post in reddit_rIndieHeads_response['data']['children'] :
    # only get posts which is after the last execution
    if datetime.datetime.strptime(format_reddit_created_date(post['data']['created_utc']), '%Y-%m-%d %H:%M:%S')  > rIndieHeads_last_time :
        success = True
        # recommendation posts usually have youtube, spotify, soundcloud, bandcamp link attached
        if 'youtu' in post['data']['url'] or 'spotify' in post['data']['url'] \
        or 'soundcloud' in post['data']['url'] or 'bandcamp' in post['data']['url'] :
            # get raw post title
            post_title_raw = post['data']['title']
            post_title_raw = html.unescape(post_title_raw)
            if ' - ' in post_title_raw:
                # get id, time and url
                post_id = post['data']['id']
                post_created_utc = format_reddit_created_date(post['data']['created_utc'])
                post_url = 'https://www.reddit.com/r/indieheads/comments/' + post_id

                # clean title text for searching purpose
                # remove '[FRESH]', '[FRESH VIDEO]', '[ORIGINAL]' from title
                post_title = post_title_raw

                if '[FRESH]' in post_title :
                    post_title = post_title.replace('[FRESH]', '')

                if '[FRESH VIDEO]' in post_title :
                    post_title = post_title.replace('[FRESH VIDEO]', '')

                if '[FRESH ALBUM]' in post_title :
                    post_title = post_title.replace('[FRESH ALBUM]', '')

                if '[ORIGINAL]' in post_title :
                    post_title = post_title.replace('[ORIGINAL]', '')

                # remove '(', '[' and it's following content

                start_searching_position = post_title.find(' - ')
                removed_positions = [post_title.find('('), post_title.find('[')]
                removed_positions.sort(reverse = True)
                for removed_position in removed_positions : 
                    if removed_position > start_searching_position :
                        post_title = post_title[:removed_position]

                # extract songs name and artist name for later validation
                # if post title already in 'validation' set, add '.' to distinguish it from previous post title
                post_title_raw_validation = post_title_raw
                if post_title_raw in validation : 
                    post_title_raw_validation = post_title_raw + ('.' * count)
                    count += 1
                song_artist_name = post_title.split(' - ')
                validation[post_title_raw_validation] = [song_artist_name[0].strip(), song_artist_name[1].strip()]
                
                # remove strange symbols exclude spaces
                post_title = re.sub('[\W_]+', ' ', post_title, flags = re.UNICODE)

                # build new row to be added
                redditDF_aux = pd.DataFrame({'post_id': [post_id], 'post_title': [post_title_raw], 
                                            'post_created_utc': [post_created_utc], 'post_url': [post_url], 
                                            'searching_context': [post_title]})
                
                # add new row to df
                redditDF = pd.concat([redditDF_aux, redditDF], ignore_index = True, axis = 0)

                if rIndieHeads_execution_time == '' :
                    rIndieHeads_execution_time = post_created_utc
    else : 
        break

# add new execution time to historic dataframe
if rIndieHeads_execution_time == '' :
    rIndieHeads_execution_time = rIndieHeads_last_time

##                                                  EXTRACT FROM r/PopHeads

# get reddit api data in json format
reddit_rPopHeads_response, reddit_rPopHeads_status_code = send_request_reddit_get_new_post('https://oauth.reddit.com/r/popheads/new/')

if reddit_rPopHeads_status_code != 200 : 
    sys.exit()

success = False
rPopHeads_execution_time = ''
for post in reddit_rPopHeads_response['data']['children'] :
    # only get posts which is after the last execution
    if datetime.datetime.strptime(format_reddit_created_date(post['data']['created_utc']), '%Y-%m-%d %H:%M:%S')  > rPopHeads_last_time :
        success = True
        # recommendation posts are all in these tags
        if post['data']['link_flair_text'] in ['[PERFORMANCE]', '[FRESH]', '[FRESH VIDEO]'] :
            # recommendation posts usually have youtube, spotify, soundcloud, bandcamp link attached
            if 'youtu' in post['data']['url'] or 'spotify' in post['data']['url'] \
            or 'soundcloud' in post['data']['url'] or 'bandcamp' in post['data']['url'] :
                # get raw post title
                post_title_raw = post['data']['title']
                post_title_raw = html.unescape(post_title_raw)
                if ' - ' in post_title_raw:
                    # get id, time and url
                    post_id = post['data']['id']
                    post_created_utc = format_reddit_created_date(post['data']['created_utc'])
                    post_url = 'https://www.reddit.com/r/popheads/comments/' + post_id

                    # clean title text for searching purpose
                    # remove '(', '[' and it's following content
                    post_title = post_title_raw

                    start_searching_position = post_title.find(' - ')
                    removed_positions = [post_title.find('('), post_title.find('[')]
                    removed_positions.sort(reverse = True)
                    for removed_position in removed_positions : 
                        if removed_position > start_searching_position :
                            post_title = post_title[:removed_position]

                    # extract songs name and artist name for later validation
                    # if post title already in 'validation' set, add '.' to distinguish it from previous post title
                    post_title_raw_validation = post_title_raw
                    if post_title_raw in validation : 
                        post_title_raw_validation = post_title_raw + ('.' * count)
                        count += 1
                    song_artist_name = post_title.split(' - ')
                    validation[post_title_raw_validation] = [song_artist_name[0].strip(), song_artist_name[1].strip()]
                    
                    # remove strange symbols exclude spaces
                    post_title = re.sub('[\W_]+', ' ', post_title, flags = re.UNICODE)

                    # build new row to be added
                    redditDF_aux = pd.DataFrame({'post_id': [post_id], 'post_title': [post_title_raw], 
                                                'post_created_utc': [post_created_utc], 'post_url': [post_url], 
                                                'searching_context': [post_title]})
                    
                    # add new row to df
                    redditDF = pd.concat([redditDF_aux, redditDF], ignore_index = True, axis = 0)

                    if rPopHeads_execution_time == '' : 
                        rPopHeads_execution_time = post_created_utc
    else : 
        break

# add new execution time to historic dataframe
if rPopHeads_execution_time == '' :
    rPopHeads_execution_time = rPopHeads_last_time

##                                                  EXTRACT FROM r/ElectronicMusic

# get reddit api data in json format
reddit_rElectronicMusic_response, reddit_rElectronicMusic_status_code = send_request_reddit_get_new_post('https://oauth.reddit.com/r/electronicmusic/new/')

if reddit_rElectronicMusic_status_code != 200 : 
    sys.exit()

success = False
rElectronicMusic_execution_time = ''
for post in reddit_rElectronicMusic_response['data']['children'] :
    # only get posts which is after the last execution
    if datetime.datetime.strptime(format_reddit_created_date(post['data']['created_utc']), '%Y-%m-%d %H:%M:%S')  > rElectronicMusic_last_time :
        success = True
        # eliminate these tags
        if post['data']['link_flair_text'] not in ['Discussion', 'AMA Announcement'] :
            # recommendation posts usually have youtube, spotify, soundcloud, bandcamp link attached
            if 'youtu' in post['data']['url'] or 'spotify' in post['data']['url'] \
            or 'soundcloud' in post['data']['url'] or 'bandcamp' in post['data']['url'] :
                # get raw post title
                post_title_raw = post['data']['title']
                post_title_raw = html.unescape(post_title_raw)
                if ' - ' in post_title_raw:
                    # get id, time and url
                    post_id = post['data']['id']
                    post_created_utc = format_reddit_created_date(post['data']['created_utc'])
                    post_url = 'https://www.reddit.com/r/electronicmusic/comments/' + post_id

                    # clean title text for searching purpose
                    # remove '(', '[' and it's following content
                    post_title = post_title_raw

                    start_searching_position = post_title.find(' - ')
                    removed_positions = [post_title.find('('), post_title.find('[')]
                    removed_positions.sort(reverse = True)
                    for removed_position in removed_positions : 
                        if removed_position > start_searching_position :
                            post_title = post_title[:removed_position]

                    # extract songs name and artist name for later validation
                    # if post title already in 'validation' set, add '.' to distinguish it from previous post title
                    post_title_raw_validation = post_title_raw
                    if post_title_raw in validation : 
                        post_title_raw_validation = post_title_raw + ('.' * count)
                        count += 1
                    song_artist_name = post_title.split(' - ')
                    validation[post_title_raw_validation] = [song_artist_name[0].strip(), song_artist_name[1].strip()]
                    
                    # remove strange symbols exclude spaces
                    post_title = re.sub('[\W_]+', ' ', post_title, flags = re.UNICODE)

                    # build new row to be added
                    redditDF_aux = pd.DataFrame({'post_id': [post_id], 'post_title': [post_title_raw], 
                                                'post_created_utc': [post_created_utc], 'post_url': [post_url], 
                                                'searching_context': [post_title]})
                    
                    # add new row to df
                    redditDF = pd.concat([redditDF_aux, redditDF], ignore_index = True, axis = 0)

                    if rElectronicMusic_execution_time == '' : 
                        rElectronicMusic_execution_time = post_created_utc
    else : 
        break

# add new execution time to historic dataframe
if rElectronicMusic_execution_time == '' :
    rElectronicMusic_execution_time = rElectronicMusic_last_time

##                                            SPOTIFY API - EXTRACTION & TRANSFORMATION 

# create header
header = get_spotify_bearer_token()

# output dataframe
mainDF = pd.DataFrame(columns = ['post_id', 'post_created_utc', 'post_title', 'post_url', 'artists_id', 
                                 'artists_name', 'track_id', 'track_name', 'release_date', 'genres', 'track_popularity'])

# contain track's feature, will merge with mainDF later
featuresDF = pd.DataFrame(columns = ['track_id', 'danceability', 'energy', 'key', 'mode', 'loudness', 'speechiness', 'acousticness', 
                                     'instrumentalness', 'liveness', 'valence', 'tempo', 'duration_ms', 'time_signature'])

for i, post in enumerate(redditDF.itertuples(index = False)) : 
    # get data from reddit dataframe
    post_id = post[redditDF.columns.get_loc('post_id')]
    post_title = post[redditDF.columns.get_loc('post_title')]
    post_created_utc = post[redditDF.columns.get_loc('post_created_utc')]
    post_url = post[redditDF.columns.get_loc('post_url')]
    searching_context = post[redditDF.columns.get_loc('searching_context')]

    # create url to search tracks
    searching_url = url_spotify_search_item(searching_context, limit = 10)

    # get spotify api data in json format
    spotify_searching_response, spotify_searching_status_code = send_spotify_get_request(searching_url[0], header, searching_url[1])
    
    have_result = False
    # check status code
    if spotify_searching_status_code == 200 : 
        # check if no result found
        if spotify_searching_response['tracks'] : 
            if spotify_searching_response['tracks']['items'] :
                for track in spotify_searching_response['tracks']['items'] :
                    # get track id, name, release date and popularity
                    track_id = track['id']
                    track_name = track['name']
                    track_release_date = track['album']['release_date']
                    track_popularity = track['popularity']

                    # get artist id and name
                    artists_id = []
                    artists_name = []
                    genres = []
                    for artist in track['artists']:
                        # get artist id
                        artists_id.append(artist['id'])
                        artists_name.append(artist['name'].replace(","," "))

                        # get genres
                        artist_url = url_spotify_get_several_artists(artist['id'])
                        artist_response, artist_status_code = send_spotify_get_request(artist_url[0], get_spotify_bearer_token(), artist_url[1])
                        for i in artist_response['artists'] : 
                            for genre in i['genres'] :
                                genres.append(genre)
                    
                    # merge list and separate with comma  
                    artists_id = ', '.join(artists_id)
                    artists_name = ', '.join(artists_name)
                    genres = ', '.join(genres)
                    
                    #validation step check if it's really a song which reddit user recommended 
                    similarity1 = similarity2 = 0
                    for i in validation[post_title] : 
                        if is_similar(track_name, i) > 0.7 : 
                            similarity1 = is_similar(track_name, i)
                        if is_similar(artists_name, i) > 0.7 :
                            similarity2 = is_similar(artists_name, i)
                    if similarity1 + similarity2 >= 1.4 :
                        # build new row to be added
                        mainDF_aux = pd.DataFrame({'post_id': [post_id], 'post_created_utc': [post_created_utc], 'post_title': [post_title], 
                                                'post_url': [post_url], 'artists_id': [artists_id], 'artists_name': [artists_name], 
                                                'track_id': [track_id], 'track_name': [track_name], 'release_date': [track_release_date], 
                                                'genres': [genres], 'track_popularity': [track_popularity],})
                        
                        # add new row to df
                        mainDF = pd.concat([mainDF, mainDF_aux], ignore_index = True, axis = 0)
                        have_result = True
                        break

    if have_result is True : 
        track_url = url_spotify_get_tracks_audio_features(track_id)
        spotify_track_response, spotify_track_status_code = send_spotify_get_request(track_url[0], header, track_url[1])
        if spotify_track_status_code == 200 : 
            if spotify_track_response['audio_features'] : 
                for features in spotify_track_response['audio_features']:
                    # get track's features
                    track_danceability = features['danceability']
                    track_energy = features['energy']
                    track_key = features['key']
                    track_loudness = features['loudness']
                    track_mode = features['mode']
                    track_speechiness = features['speechiness']
                    track_acousticness = features['acousticness']
                    track_instrumentalness = features['instrumentalness']
                    track_liveness = features['liveness']
                    track_valence = features['valence']
                    track_tempo = features['tempo']
                    track_duration_ms = features['duration_ms']
                    track_time_signature = features['time_signature']

                    # build new row to be added
                    featuresDF_aux = pd.DataFrame({'track_id': [track_id], 'danceability': [track_danceability], 'energy': [track_energy], 
                                                   'key': [track_key], 'loudness': [track_loudness], 'mode': [track_mode], 
                                                   'speechiness': [track_speechiness], 'acousticness': [track_acousticness], 
                                                   'instrumentalness': [track_instrumentalness], 'liveness': [track_liveness], 
                                                   'valence': [track_valence], 'tempo': [track_tempo], 
                                                   'duration_ms': [track_duration_ms], 'time_signature': [track_time_signature]})

                    # add new row to df
                    featuresDF = pd.concat([featuresDF, featuresDF_aux], ignore_index = True, axis = 0)
# merge 2 above df 
# mainDF = mainDF.merge(featuresDF, on = 'track_id', how = 'inner')
mainDF = mainDF.join(featuresDF.set_index('track_id'), on = 'track_id')

# in case 1 title was posted many times
mainDF.drop_duplicates(subset = "post_id", inplace = True)

##                                                      LOADING

# get date and time to create data file's name
todays_date = datetime.datetime.today().strftime('%Y%m%d%H')
export_file_path = os.path.join(os.getcwd(), f'src/spark/data/{todays_date}_reddit.csv')

# create csv output file
mainDF.to_csv(export_file_path, mode = 'a', sep = ',', encoding = 'utf-8', index = False, header = True)

# add historic data to csv file
historyDF = pd.DataFrame({'rMusic': [rMusic_execution_time], 'rIndieHeads': [rIndieHeads_execution_time], 
                          'rPopHeads': [rPopHeads_execution_time], 'rElectronicMusic': [rElectronicMusic_execution_time]})

historyDF.to_csv(history_file, mode = 'a', sep = ',', encoding = 'utf-8', index = False, header = False)