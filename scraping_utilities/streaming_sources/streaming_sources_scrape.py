# Before running this code move streaming_sources.csv from folder streaming_sources

import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from datetime import datetime, date
import yaml
from urllib.parse import unquote
from urllib import request
import urllib
import numpy as np
import time
from multiprocessing import Pool
import sqlalchemy
import re
import requests
from threading import Thread

import sys
sys.path.extend(['./..'])
from utilities import keep_connection_alive_for_scraping


thread = Thread(target=keep_connection_alive_for_scraping)
thread.start()

config = yaml.safe_load(open('./../../config.yml'))
data_folder = config['streaming_sources']


def parallelize_dataframe(df, func, n_cores=config['algo']['vCPU']):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


try:
    df_justwatch_contents = pd.read_csv('/tmp/justwatch_countries_contents.csv')
except:
    countries = config['scrape_data']['countries'].copy()

    print('Collecting country wise platforms...')
    for key, value in countries.items():
        print(key, value['name'])
        url_to_scrape = 'https://apis.justwatch.com/content/providers/locale/'+key
        response = request.urlopen(url_to_scrape)
        response = eval(response.read().decode('utf8').replace('true', 'True').replace('false', 'False').replace('null', 'None'))
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
    df_justwatch_contents = pd.DataFrame(combos)
    print('Pages to scrape -', df_justwatch_contents.shape[0])


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
            response = requests.get(url)
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


    def get_content_data(url):
        try:
            response = request.urlopen(url)
            response = eval(
                response.read().decode('utf8').replace('true', 'True').replace('false', 'False').replace('null', 'None'))

            title = response.get('title')
            url = response.get('full_path')
            release_year = response.get('original_release_year')
            streaming_info = response.get('offers')
            clips = response.get('clips')
            external_ids = response.get('external_ids')

            return title, url, release_year, streaming_info, clips, external_ids
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





##################################################################################
# Cleaning scraped data
##################################################################################





print('\nStarting to clean scraped data...\n')

print('Getting IMDb ids...')


def get_imdb_id(external_ids):
    imdb_id = None
    for external_id in external_ids:
        if external_id.get('provider') == 'imdb':
            imdb_id = external_id['external_id']
    return imdb_id


df_justwatch_contents['imdb_id'] = None
df_justwatch_contents['imdb_id'][pd.notnull(df_justwatch_contents['external_ids'])] = df_justwatch_contents['external_ids'][pd.notnull(df_justwatch_contents['external_ids'])].apply(get_imdb_id)
del df_justwatch_contents['external_ids']

engine = sqlalchemy.create_engine(
    'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
        'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])
conn = engine.connect()

df_db_ids = pd.read_sql("""
                            select imdb_content_id,
                                   title,
                                   release_year,
                                   case when type = 'tv' then 'show' else type end as item_type
                            from """ + config['sql']['schema'] + """.content_details
                            order by num_votes desc
                        """, con=conn)
df_db_ids.drop_duplicates(['title', 'release_year', 'item_type'], inplace=True)

df_justwatch_contents = pd.merge(df_justwatch_contents,
                                 df_db_ids,
                                 how='left',
                                 on=['title', 'release_year', 'item_type'])
df_justwatch_contents['imdb_id'][pd.isnull(df_justwatch_contents['imdb_id'])] = \
df_justwatch_contents['imdb_content_id'][pd.isnull(df_justwatch_contents['imdb_id'])]
del df_justwatch_contents['imdb_content_id']

print('Getting youtube trailer ids...')


def get_youtube_trailer_id(clips):
    trailer_id = None
    for clip in clips:
        if (clip.get('type') == 'trailer') and (clip.get('name').lower().count('trailer') != 0) and (
                clip.get('provider') == 'youtube'):
            trailer_id = clip['external_id']
            break
    return trailer_id


df_justwatch_contents['trailer_id'] = None
df_justwatch_contents['trailer_id'][pd.notnull(df_justwatch_contents['clips'])] = df_justwatch_contents['clips'][
    pd.notnull(df_justwatch_contents['clips'])].apply(get_youtube_trailer_id)
del df_justwatch_contents['clips']

print('Parsing streaming info...')


def parse_streaming_info(row):
    for idx, item in enumerate(row['streaming_info']):
        row['streaming_info'][idx] = {
            'justwatch_id': row['justwatch_id'],
            'country_code': row['country_code'],
            'monetization_type': item['monetization_type'],
            'provider_id': item['provider_id'],
            'presentation_type': item['presentation_type'],
            'streaming_link': item.get('urls', {
                'standard_web': item['streaming_link'] if 'streaming_link' in item.keys() else None}).get(
                'standard_web')
        }

    return row['streaming_info']


df_justwatch_contents['streaming_info'][pd.notnull(df_justwatch_contents['streaming_info'])] = df_justwatch_contents[pd.notnull(df_justwatch_contents['streaming_info'])].apply(parse_streaming_info, axis=1)

print('Building streaming info dataframe...')
df_justwatch_contents = df_justwatch_contents[pd.notnull(df_justwatch_contents['streaming_info'])]


def build_streaming_info_dataframe(df):
    return pd.DataFrame(df['streaming_info'].sum())


df_streaming_info = parallelize_dataframe(df_justwatch_contents, build_streaming_info_dataframe)
df_streaming_info = df_streaming_info[df_streaming_info['monetization_type'] != 'cinema']
df_streaming_info = df_streaming_info[pd.notnull(df_streaming_info['streaming_link'])]

print('Removing extra streaming info...')
countries = config['scrape_data']['countries'].copy()
country_platform_combo = []
for key, value in countries.items():
    for platform in value['platforms']:
        country_platform_combo.append(key+'^'+str(platform))


df_streaming_info['country_code__provider_id'] = df_streaming_info.apply(lambda row: row['country_code']+'^'+str(row['provider_id']), axis=1)
old_count = df_streaming_info.shape[0]
df_streaming_info = df_streaming_info[df_streaming_info['country_code__provider_id'].isin(country_platform_combo)]
del df_streaming_info['country_code__provider_id']
print(old_count, '-->', df_streaming_info.shape[0])

print('Cleaning streaming links...')


def clean_streaming_links(link):
    if (link.lower().count('justwatch') != 0) or (link.lower().count('just-watch') != 0) or (
            link.count('tag=just') != 0) or (link.count('camp=') != 0):
        if link.count('flixfling.com') != 0:
            link = link.replace('justwatch', 'movie')
        if (link.count('pantaflix.com') != 0) or (link.count('docplay.com') != 0):
            link = link.split('?')[0]
        if (link.count('camp=') != 0) or (link.count('tag=just') != 0):
            link = link.split('?')[0]
    return link


df_streaming_info['streaming_link'][pd.notnull(df_streaming_info['streaming_link'])] = df_streaming_info['streaming_link'][pd.notnull(df_streaming_info['streaming_link'])].apply(clean_streaming_links)

df_streaming_info['monetization_type'] = df_streaming_info['monetization_type'].apply(
    lambda x: 'stream' if x in ['flatrate', 'ads', 'free'] else x)

quality_ranking = {
    '4k': 1,
    'hd': 2,
    'bluray': 3,
    'sd': 4,
    'dvd': 5
}
df_streaming_info['quality_ranking'] = df_streaming_info['presentation_type'].apply(lambda x: quality_ranking[x])
df_streaming_info.sort_values('quality_ranking', inplace=True)
df_streaming_info.drop_duplicates(['country_code', 'justwatch_id', 'monetization_type', 'provider_id'], inplace=True)
del df_streaming_info['quality_ranking']
del df_streaming_info['presentation_type']

print('Getting platform names...')
countries = config['scrape_data']['countries'].copy()

platforms = []
for key, value in countries.items():
    url_to_scrape = 'https://apis.justwatch.com/content/providers/locale/' + key
    response = request.urlopen(url_to_scrape)
    response = eval(
        response.read().decode('utf8').replace('true', 'True').replace('false', 'False').replace('null', 'None'))
    for platform in response:
        platforms.append({
            'provider_id': platform['id'],
            'provider_name': platform['clear_name'].lower().replace(' ', '_') if platform['id'] not in [336, 345] else {336: 'sky_espana', 345: 'canal+_series'}[platform['id']]
        })
df_platforms = pd.DataFrame(platforms)
df_platforms.drop_duplicates(inplace=True)

df_streaming_info = pd.merge(df_streaming_info, df_platforms, how='left', on='provider_id')
df_streaming_info['country_name'] = df_streaming_info['country_code'].apply(
    lambda x: countries[x]['name'].lower().replace(' ', '_'))
del df_streaming_info['country_code']
del df_streaming_info['provider_id']

print('Grouping streaming links into final format...')
df_streaming_info['streaming_info'] = df_streaming_info.apply(lambda row: {row['provider_name']: row['streaming_link']},
                                                              axis=1)

df_streaming_info = df_streaming_info.groupby(
    ['justwatch_id', 'country_name', 'monetization_type']).streaming_info.apply(list).reset_index()


def list_to_dict(items):
    output_dict = {}
    for item in items:
        output_dict.update(item)
    return output_dict


df_streaming_info['streaming_info'][pd.notnull(df_streaming_info['streaming_info'])] = df_streaming_info['streaming_info'][pd.notnull(df_streaming_info['streaming_info'])].apply(lambda x: list_to_dict(x))

df_streaming_info['streaming_info'] = df_streaming_info.apply(
    lambda row: {row['monetization_type']: row['streaming_info']}, axis=1)
del df_streaming_info['monetization_type']

df_streaming_info = df_streaming_info.groupby(['justwatch_id', 'country_name']).streaming_info.apply(list).reset_index()
df_streaming_info['streaming_info'][pd.notnull(df_streaming_info['streaming_info'])] = df_streaming_info['streaming_info'][pd.notnull(df_streaming_info['streaming_info'])].apply(lambda x: list_to_dict(x))

df_streaming_info['country_name'] = df_streaming_info['country_name'].apply(lambda x: 'where_to_watch_' + x)
df_streaming_info = df_streaming_info.pivot(index='justwatch_id', columns='country_name',
                                            values='streaming_info').reset_index()
del df_streaming_info.columns.name

df_streaming_info = pd.merge(df_streaming_info,
                             df_justwatch_contents[['justwatch_id', 'imdb_id', 'trailer_id']].drop_duplicates('justwatch_id'),
                             how='left',
                             on='justwatch_id')
del df_streaming_info['justwatch_id']
df_streaming_info = df_streaming_info[pd.notnull(df_streaming_info['imdb_id'])]
df_streaming_info = df_streaming_info.drop_duplicates('imdb_id').reset_index(drop=True)
df_streaming_info = df_streaming_info[[
    'imdb_id',
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
]]

df_streaming_info.to_csv('/home/ec2-user/scraped/streaming_info.csv', sep='^', index=False)
