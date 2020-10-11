import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from datetime import datetime, date
import yaml
from urllib.parse import unquote
import urllib
import numpy as np
import sys
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

indices = [int(x) for x in sys.argv[-1].split('-')]

df_justwatch_contents = pd.read_csv('/tmp/streaming_urls.csv', sep='^')
df_justwatch_contents = df_justwatch_contents.iloc[indices[0]:indices[1], :]


def parallelize_dataframe(df, func, n_cores=2):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def get_content_data(url):
    try:
        session = get_session()
        response = session.get(url)
        if response.status_code == 429:
            print('Received status code 429, sleeping for 5 seconds...')
            time.sleep(5)
            response = session.get(url)
        response = response.json()
        session.close()

        title = response.get('title')
        url1 = response.get('full_path')
        release_year = response.get('original_release_year')
        streaming_info = response.get('offers')
        clips = response.get('clips')
        external_ids = response.get('external_ids')

        return title, url1, release_year, streaming_info, clips, external_ids
    except Exception as ex:
        print(ex)
        print('Sleeping for 1 second...\n')
        time.sleep(1)
        return None, None, None, None, None, None


def apply_get_content_data(df):
    df['title'], df['url'], df['release_year'], df['streaming_info'], df['clips'], df['external_ids'] = zip(*df['url'].apply(get_content_data))
    return df


print('Collecting content streaming info...')
df_justwatch_contents = parallelize_dataframe(df_justwatch_contents, apply_get_content_data)
df_justwatch_contents.to_csv('/tmp/justwatch_countries_contents.csv', index=False)

df_justwatch_contents = pd.read_csv('/tmp/justwatch_countries_contents.csv')

df_justwatch_contents['external_ids'][pd.notnull(df_justwatch_contents['external_ids'])] = df_justwatch_contents['external_ids'][pd.notnull(df_justwatch_contents['external_ids'])].apply(eval)
df_justwatch_contents['clips'][pd.notnull(df_justwatch_contents['clips'])] = df_justwatch_contents['clips'][pd.notnull(df_justwatch_contents['clips'])].apply(eval)
df_justwatch_contents['streaming_info'][pd.notnull(df_justwatch_contents['streaming_info'])] = df_justwatch_contents['streaming_info'][pd.notnull(df_justwatch_contents['streaming_info'])].apply(eval)

df_justwatch_contents['justwatch_id'] = df_justwatch_contents.apply(
    lambda row:str(row['justwatch_id'])+'^'+re.sub(r'[^a-z0-9]+', '-', str(row['title']), flags=re.I).lower()+'^'+str(row['release_year']),
axis=1)

df_justwatch_contents.to_csv('/home/ec2-user/scraped/streaming_info.csv', sep='^', index=False)
