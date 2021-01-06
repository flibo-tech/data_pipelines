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


countries = config['scrape_data']['countries'].copy()

print('Collecting country wise platforms...')
for key, value in countries.items():
    print(key, value['name'])
    url_to_scrape = 'https://apis.justwatch.com/content/providers/locale/'+key
    session = get_session()
    response = session.get(url_to_scrape).json()
    session.close()
    countries[key] = {
        'country_name': countries[key]['name'],
        'platforms': [platform['short_name'] for platform in response]
    }

start_year = 1900
this_year = date.today().year
years = [1900+i for i in range(this_year-start_year+1)]

combos = []
for key, value in countries.items():
    for year in years:
        for content_type in ['movie', 'show']:
            combos.append({
                'country_code': key,
                'country_name': value['country_name'],
                'platforms': value['platforms'],
                'content_type': content_type,
                'start_year': year,
                'end_year': year+1
            })
df_justwatch_contents = pd.DataFrame(combos).head(36)

df_justwatch_contents.to_csv('/home/ec2-user/scraped/streaming_url_combos.csv', sep='^', index=False)
