import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import yaml
import sqlalchemy
from multiprocessing import Pool
import numpy as np
import requests
import time
from random import shuffle
from json.decoder import JSONDecodeError


config = yaml.safe_load(open('./../config.yml'))
tmdb_api_key = config['tmdb_api_key']


def parallelize_dataframe(df, func, n_cores=config['algo']['vCPU']):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def get_images(imdb_person_id):
    tmdb_api_keys = ['57b66bd8b82b9db257af4184ca3f5e8d', '8955de4a0f1c29f7f7f36604a33dda5a', 'c14e5706c5c83c58873f3223a58669c3',
                     'b99f9c8de2ac177b4f4e93510f2244a2', '8e97a8723f3405c373541c5140da2698', 'f0776fdd5f16df5639f3deb28ae20add',
                     '0bfa442ef7bd25b1323c003a19b6313f']
    shuffle(tmdb_api_keys)
    external_id_url = 'https://api.themoviedb.org/3/find/' + imdb_person_id + '?api_key=' + tmdb_api_keys[0] + '&language=en-US&external_source=imdb_id'
    response = requests.get(external_id_url)
    if response.status_code == 429:
        time.sleep(2)
        try:
            response = requests.get(external_id_url)
            response = response.json().get('person_results')
        except Exception as ex:
            print(ex)
            return 'Error', 'Error', 'Error', 'Error'
    else:
        try:
            response = response.json().get('person_results')
        except JSONDecodeError:
            print('JSONDecodeError')
            return 'Error', 'Error', 'Error', 'Error'

    if response:
        response = response[0]
        return response.get('id'), response.get('name'), response.get('gender'), response.get('profile_path')
    else:
        return None, None, None, None


def apply_get_images(df):
    df['fetched_tmdb_id'], df['fetched_name'], df['gender'], df['picture'] = zip(*df['imdb_person_id'].apply(get_images))
    return df


engine = sqlalchemy.create_engine(
    'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
        'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])
conn = engine.connect()

df_artists = pd.read_sql("""
                            select person_id, imdb_person_id, "name"
                            from app.artists
                            where person_id in (
                                                select distinct person_id
                                                from app.content_crew
                                                where credit_order <= 10
                                                )
                        """, con=conn)

print('Collecting images...')
df_artist_images = parallelize_dataframe(df_artists, apply_get_images)
df_artist_images.to_csv('artist_images.csv', index=False)
