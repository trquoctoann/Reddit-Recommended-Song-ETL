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