from justwatch import JustWatch
import pandas as pd


def stream_online(title):
    just_watch = JustWatch(country='AU')
    results = JustWatch().search_for_item(query=title[1], providers=None, content_types=['movie'])
    resp = None
    items = results.get('items', None)
    if items:
        for item in items:
            if (item.get('title', item.get('original_title', 'No movie name found')).lower() == title[1].lower()) & (item['original_release_year'] == title[2]):
                resp = item
                break
    return resp


# df_ratings_n_stream = pd.read_csv('movie_ratings_n_stream_30k.csv')
# titles_collected = list(df_ratings_n_stream['title_id'].unique())
# titles = list(df_movies.apply(lambda row: (row['tconst'], row['primaryTitle'], row['startYear']), axis=1).unique())
# titles = list(df_movies['tconst'].unique())
# titles = titles[:30000]
# titles = [title for title in titles if title[0] not in titles_collected]
titles = ['tt4154756']
# del df_ratings_n_stream
df = pd.DataFrame()
i = 0 # len(titles_collected)
# print(i)
for title in titles:
    resp = stream_online(title)
    df = df.append([{'title_id':title[0], 'data':resp}])
    i += 1
    if i%100 == 0:
        try:
            df_ratings_n_stream = pd.read_csv('movie_ratings_n_stream_30k.csv')
        except:
            df_ratings_n_stream = pd.DataFrame()
        df_ratings_n_stream = pd.concat([df_ratings_n_stream,df], axis=0)
        df_ratings_n_stream.to_csv('movie_ratings_n_stream_30k.csv', index=False)
        print(i)
        del df_ratings_n_stream
        df = pd.DataFrame()


df = pd.read_csv('./cleaned_data/movie_just_watch_30k.csv')

def ratings(data):
    ratings = []
    if type(data)==str:
        data = eval(data)
        scoring = data.get('scoring', None)
        if scoring:
            for item in scoring:
                temp_dict = {}
                temp_dict[item['provider_type'].replace(':','_')] = item['value']
                ratings.append(temp_dict)
    return ratings
df['ratings'] = df['data'].apply(ratings)


def stream(data):
    urls = []
    if type(data)==str:
        data = eval(data)
        offers = data.get('offers', None)
        if offers:
            for offer in offers:
                url = offer['urls']['standard_web']
                if url not in urls:
                    urls.append(url)
    providers = ['mubi', 'dendydirect', 'stan', 'microsoft', 'playstation', 'quickflix', 'itunes', 'google', 'netflix',
                 'crackle']
    where_to_watch = {}
    for provider in providers:
        where_to_watch[provider] = None
        check = False
        for url in urls:
            if url.count(provider + '.') != 0:
                where_to_watch[provider] = url
                check = True
                break
    return where_to_watch
df['where_to_watch'] = df['data'].apply(stream)


def dict_for_df(row):
    temp_dict = {}
    temp_dict['title_id'] = row['title_id']
    for rating in row['ratings']:
        temp_dict.update(rating)
    temp_dict.update({'where_to_watch': str(row['where_to_watch'])})
    
    return [temp_dict]
df['dict_for_df'] = df.apply(dict_for_df, axis=1)

df_ratings_n_stream = pd.DataFrame(df['dict_for_df'].sum())

df_ratings_n_stream.to_csv('./cleaned_data/movie_ratings_n_stream_30k.csv', index=False)