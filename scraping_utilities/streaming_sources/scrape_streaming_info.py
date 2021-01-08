import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from datetime import datetime
import yaml
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
        if 'streaming_info.csv' in files:
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
df_justwatch_contents['streaming_info'][pd.notnull(df_justwatch_contents['streaming_info'])] = df_justwatch_contents['streaming_info'][pd.notnull(df_justwatch_contents['streaming_info'])].apply(list)

df_justwatch_contents['justwatch_id'] = df_justwatch_contents.apply(
    lambda row:str(row['justwatch_id'])+'^'+re.sub(r'[^a-z0-9]+', '-', str(row['title']), flags=re.I).lower()+'^'+str(row['release_year']),
axis=1)

print('\nStarting to clean scraped data...\n')

print('Getting IMDb ids...')


def get_imdb_id(external_ids):
    imdb_ids = []
    for external_id in external_ids:
        if (external_id.get('provider') == 'imdb') and str(external_id['external_id']).count('tt'):
            imdb_ids.append(external_id['external_id'])

    if not imdb_ids:
        imdb_ids = [None]

    return imdb_ids


df_justwatch_contents['imdb_ids'] = None
df_justwatch_contents['imdb_ids'][pd.notnull(df_justwatch_contents['external_ids'])] = df_justwatch_contents['external_ids'][pd.notnull(df_justwatch_contents['external_ids'])].apply(get_imdb_id)
del df_justwatch_contents['external_ids']

df_justwatch_contents = df_justwatch_contents.where((pd.notnull(df_justwatch_contents)), None)


def flatten_imdb_ids(row):
    output = []
    for imdb_id in (row['imdb_ids'] or [None]):
        row['imdb_id'] = imdb_id
        output.append(row.copy())
    return output


print('Flattening imdb ids...')
df_justwatch_contents['temp'] = df_justwatch_contents.apply(lambda row: flatten_imdb_ids(row), axis=1)
df_justwatch_contents = pd.DataFrame(df_justwatch_contents['temp'].sum()).reset_index(drop=True)
del df_justwatch_contents['imdb_ids']
print('Done flattening.\n')

df_db_ids = pd.read_csv('/tmp/content_metainfo.csv', sep='^')
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
    if link.count('app.primevideo.com') != 0:
        link = link.replace('app.primevideo.com', 'primevideo.com')
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
    session = get_session()
    response = session.get(url_to_scrape).json()
    session.close()
    for platform in response:
        platforms.append({
            'provider_id': platform['id'],
            'provider_name': platform['clear_name'].lower().replace(' ', '_') if platform['id'] not in [336, 345] else {336: 'sky_espana', 345: 'canal+_series'}[platform['id']]
        })
df_platforms = pd.DataFrame(platforms)
df_platforms.drop_duplicates(inplace=True)

df_streaming_info = pd.merge(df_streaming_info, df_platforms, how='left', on='provider_id')
df_streaming_info = df_streaming_info[pd.notnull(df_streaming_info['provider_name'])]

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
                             df_justwatch_contents[['justwatch_id', 'imdb_id', 'trailer_id']].drop_duplicates(['justwatch_id', 'imdb_id']),
                             how='left',
                             on='justwatch_id')
del df_streaming_info['justwatch_id']
df_streaming_info = df_streaming_info[pd.notnull(df_streaming_info['imdb_id'])]
df_streaming_info = df_streaming_info.drop_duplicates('imdb_id').reset_index(drop=True)

for col in [
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
]:
    if col not in df_streaming_info.columns:
        df_streaming_info[col] = None

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

df_streaming_info.to_csv('/home/ec2-user/scraped/streaming_info_temp.csv', sep='^', index=False)

print('Removing wrong JIO links...')
# imdb ids where jio links need to be removed
imdb_ids = ['tt0246986', 'tt0155774', 'tt0355784', 'tt0297241', 'tt0072190', 'tt0295297', 'tt0366816', 'tt0466901', 'tt0114369', 'tt0307156', 'tt0470283', 'tt0108329', 'tt7686456', 'tt0423286', 'tt0094621', 'tt7471004', 'tt6903234', 'tt8616144', 'tt5017396', 'tt4014320', 'tt9031094', 'tt4116284', 'tt0071315', 'tt0312859', 'tt6546758', 'tt7151510', 'tt2265544', 'tt0367495', 'tt0103874', 'tt5548032', 'tt5541338', 'tt1106860', 'tt0097416', 'tt0093603', 'tt2542406', 'tt0099043', 'tt0352277', 'tt0082096', 'tt0211915', 'tt0359950', 'tt7717938', 'tt0051792', 'tt2727028', 'tt7180544', 'tt0049366', 'tt2556308', 'tt1144804', 'tt0237038', 'tt0082933', 'tt0329717', 'tt4814290', 'tt2309600', 'tt0091559', 'tt0119978', 'tt0079368', 'tt0087182', 'tt0367110', 'tt0108399', 'tt7605494', 'tt5592256', 'tt0962746', 'tt0352757', 'tt0071707', 'tt0246809', 'tt0118931', 'tt0266875', 'tt0098491', 'tt0290429', 'tt0977636', 'tt1612039', 'tt0152720', 'tt1740092', 'tt2777548', 'tt0197294', 'tt0046164', 'tt0142521', 'tt0152080', 'tt0139110', 'tt0318593', 'tt0298492', 'tt0046695', 'tt0055724', 'tt1736647', 'tt0393724', 'tt0187178', 'tt0315255', 'tt0274604', 'tt2199330', 'tt1310501', 'tt0098615', 'tt9597734', 'tt0274026', 'tt0271572', 'tt5523516', 'tt0103867', 'tt0266486', 'tt4084182', 'tt1813225', 'tt0282139', 'tt0155618', 'tt0245768', 'tt0463939', 'tt6782838', 'tt6358168', 'tt0243559', 'tt0257385', 'tt8139156', 'tt0071072', 'tt0045453', 'tt2359640', 'tt0383702', 'tt1461683', 'tt1149252', 'tt0246052', 'tt1943756', 'tt0886672', 'tt0390587', 'tt3395024', 'tt1582477', 'tt5908938', 'tt0264056', 'tt0140448', 'tt0314942', 'tt0119174', 'tt2187114', 'tt8669128', 'tt0984177', 'tt3453512', 'tt1830477', 'tt0096446', 'tt0119385', 'tt1806959', 'tt4316170', 'tt0102057', 'tt0187574', 'tt0116274', 'tt1194236', 'tt0259170', 'tt1600439', 'tt0117500', 'tt6754688', 'tt0093773', 'tt1540133', 'tt4635548', 'tt0997033', 'tt2091384', 'tt6814630', 'tt0116704', 'tt0475632', 'tt0079098', 'tt3142232', 'tt0268117', 'tt0155928', 'tt2287973', 'tt1814797']

df = pd.read_csv('/home/ec2-user/scraped/streaming_info_temp.csv', sep='^')


def remove_jio_link(streaming_info):
    streaming_info.get('stream', {}).pop('jio_cinema', None)

    if not list(streaming_info.get('stream', {}).keys()):
        streaming_info.pop('stream', None)

    if not list(streaming_info.keys()):
        streaming_info = None

    return streaming_info


df['where_to_watch_india'][df['imdb_id'].isin(imdb_ids)] = df['where_to_watch_india'][df['imdb_id'].isin(imdb_ids)].apply(
    lambda x: remove_jio_link(eval(x)) if str(x).lower() not in ['none', 'nan'] else x
)

df = df[pd.notnull(df['imdb_id'])]

df.to_csv('/home/ec2-user/scraped/streaming_info.csv', sep='^', index=False)
