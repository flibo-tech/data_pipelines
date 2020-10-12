import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from datetime import datetime, date
import yaml
from urllib.parse import unquote
import urllib
import numpy as np
import time
from multiprocessing import Pool
import sqlalchemy
import re
import requests
from threading import Thread
import os


def get_session(proxy=None):
    session = requests.Session()
    session.headers.update({
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'
    })
    if proxy:
        session.proxies.update({
                'http': 'http://' + proxy,
                'https': 'https://' + proxy,
                'ftp': 'ftp://' + proxy
            })

    return session


def keep_connection_alive_for_scraping():
    start_time = datetime.now()
    keep_alive = True
    while keep_alive:
        time_since_start = (datetime.now() - start_time).seconds
        if time_since_start < 60:
            time_since_start = str(time_since_start) + ' seconds'
        elif time_since_start < 3600:
            time_since_start = str(time_since_start // 60) + ':' + str(time_since_start % 60) + ' minutes'
        else:
            time_since_start = str(time_since_start // 3600) + ':' + str((time_since_start % 3600) // 60) + ' hours'

        print('Keeping connection alive after '+time_since_start+'...')

        time.sleep(30)
        try:
            files = os.listdir('/home/ec2-user/scraped/')
        except FileNotFoundError:
            files = []
        if files:
            keep_alive = False

    return True


thread = Thread(target=keep_connection_alive_for_scraping)
thread.start()

config = yaml.safe_load(open('./../../config.yml'))
data_folder = config['streaming_sources']


def parallelize_dataframe(df, func, n_cores=2):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


print('concatenating CSVs...')
df_justwatch_contents = pd.DataFrame()
for filename in os.listdir('/tmp/'):
    if filename.startswith('streaming_info_') and filename.endswith('.csv'):
        print(filename)
        df_justwatch_contents = pd.concat([
            df_justwatch_contents, pd.read_csv('/tmp/'+filename, sep='^')
        ], axis=0)
print('Size -', df_justwatch_contents.shape[0])

print('Removing null imdb ids...')
df_justwatch_contents = df_justwatch_contents[pd.notnull(df_justwatch_contents['imdb_id'])]

columns = [
    'trailer_id',
    'where_to_watch_australia',
    'where_to_watch_brazil',
    'where_to_watch_canada',
    'where_to_watch_france',
    'where_to_watch_germany',
    'where_to_watch_india',
    'where_to_watch_indonesia',
    'where_to_watch_italy',
    'where_to_watch_japan',
    'where_to_watch_mexico',
    'where_to_watch_philippines',
    'where_to_watch_russia',
    'where_to_watch_spain',
    'where_to_watch_united_kingdom',
    'where_to_watch_united_states'
]

print('Replacing null in other columns with empty string...')
for col in columns:
    df_justwatch_contents[col][pd.isnull(df_justwatch_contents[col])] = ''

df_justwatch_contents = df_justwatch_contents.groupby('imdb_id').agg({
    'trailer_id': 'max',
    'where_to_watch_australia': 'max',
    'where_to_watch_brazil': 'max',
    'where_to_watch_canada': 'max',
    'where_to_watch_france': 'max',
    'where_to_watch_germany': 'max',
    'where_to_watch_india': 'max',
    'where_to_watch_indonesia': 'max',
    'where_to_watch_italy': 'max',
    'where_to_watch_japan': 'max',
    'where_to_watch_mexico': 'max',
    'where_to_watch_philippines': 'max',
    'where_to_watch_russia': 'max',
    'where_to_watch_spain': 'max',
    'where_to_watch_united_kingdom': 'max',
    'where_to_watch_united_states': 'max'
}).reset_index()

print('Size reduced to-', df_justwatch_contents.shape[0])

print('Replacing empty string in other columns with null...')
for col in columns:
    df_justwatch_contents[col][df_justwatch_contents[col] == ''] = None

print('Dumping collated streaming info...')
df_justwatch_contents.to_csv('/home/ec2-user/scraped/final_streaming_info.csv', sep='^', index=False)
