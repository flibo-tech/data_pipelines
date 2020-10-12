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
import sys


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

df_justwatch_contents = pd.read_csv('/tmp/streaming_url_combos.csv', sep='^')
df_justwatch_contents = df_justwatch_contents.iloc[indices[0]:indices[1], :]
print('Pages to scrape -', df_justwatch_contents.shape[0])


def parallelize_dataframe(df, func, n_cores=2):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


countries = config['scrape_data']['countries'].copy()


def get_contents(row):
    url_part_1 = """https://apis.justwatch.com/content/titles/"""
    url_part_2 = """/popular?"""
    url_part_3 = '''{"age_certifications":[],"content_types":["'''
    url_part_4 = '''"],"genres":[],"languages":null,"min_price":null,"max_price":null,"monetization_types":["ads","buy","flatrate","free","rent"],"presentation_types":[],"providers":'''
    url_part_5 = ""","release_year_from":"""
    url_part_6 = ""","release_year_until":"""
    url_part_7 = ""","scoring_filter_types":null,"timeline_type":null,"q":null,"person_id":null,"sort_by":null,"sort_asc":null,"query":null,"page":"""
    url_part_8 = ""","page_size":100}"""

    base_url = url_part_3+ \
               row['content_type'] + \
               url_part_4 + \
               str(row['platforms']).replace("'", '"')+\
               url_part_5+\
               str(row['start_year'])+\
               url_part_6+\
               str(row['end_year'])+\
               url_part_7

    current_page = 1
    response_items = []
    collect_more_urls = True
    while collect_more_urls:
        url = url_part_1+row['country_code']+url_part_2+urllib.parse.urlencode({'body': base_url+str(current_page)+url_part_8})
        session = get_session()
        response = session.get(url)
        session.close()
        if response.status_code == 200:
            response = response.json()
            if response['items']:
                response_items = response_items + [{
                    'justwatch_id': x.get('id'),
                    'item_type': x.get('object_type')
                } for x in response['items']]
                current_page += 1
            else:
                collect_more_urls = False
        else:
            print(response.status_code)
            collect_more_urls = False

    return response_items


def apply_get_contents(df):
    df['contents'] = df.apply(lambda row: get_contents(row), axis=1)
    return df


print('Collecting justwatch IDs...')
df_justwatch_contents = parallelize_dataframe(df_justwatch_contents, apply_get_contents)


def add_country_code(row):
    contents = row['contents']
    for content in contents:
        content.update({'country_code': row['country_code']})
        content.update({'country_name': row['country_name']})
    return contents


df_justwatch_contents['contents'] = df_justwatch_contents.apply(lambda row: add_country_code(row), axis=1)
df_justwatch_contents = pd.DataFrame(df_justwatch_contents['contents'].sum())
df_justwatch_contents.drop_duplicates(inplace=True)

df_justwatch_contents['url'] = None
df_justwatch_contents['url'] = df_justwatch_contents.apply(lambda row: 'https://apis.justwatch.com/content/titles/'+row['item_type']+'/'+str(row['justwatch_id'])+'/locale/'+row['country_code']+'?language=en', axis=1)

df_justwatch_contents.to_csv('/home/ec2-user/scraped/streaming_urls.csv', sep='^', index=False)
