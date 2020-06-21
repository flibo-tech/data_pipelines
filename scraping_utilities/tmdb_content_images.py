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


config = yaml.safe_load(open('./../config.yml'))
tmdb_api_key = config['tmdb_api_key']


def parallelize_dataframe(df, func, n_cores=config['algo']['vCPU']):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def get_images(imdb_content_id, content_type):
    tmdb_api_keys = ['57b66bd8b82b9db257af4184ca3f5e8d', '8955de4a0f1c29f7f7f36604a33dda5a', 'c14e5706c5c83c58873f3223a58669c3',
                     'b99f9c8de2ac177b4f4e93510f2244a2', '8e97a8723f3405c373541c5140da2698', 'f0776fdd5f16df5639f3deb28ae20add',
                     '0bfa442ef7bd25b1323c003a19b6313f']
    shuffle(tmdb_api_keys)
    external_id_url = 'https://api.themoviedb.org/3/find/' + imdb_content_id + '?api_key=' + tmdb_api_keys[0] + '&language=en-US&external_source=imdb_id'
    response = requests.get(external_id_url)
    if response.status_code == 429:
        time.sleep(2)
        try:
            response = requests.get(external_id_url)
            response = response.json().get(content_type+'_results')
        except:
            return 'Error', 'Error', 'Error', 'Error', 'Error', 'Error', 'Error'
    else:
        response = response.json().get(content_type + '_results')

    if response:
        response = response[0]
        return response.get('id'), response.get('title'), response.get('original_title'), response.get('original_language'), response.get('overview'), response.get('poster_path'), response.get('backdrop_path')
    else:
        return None, None, None, None, None, None, None


def apply_get_images(df):
    df['fetched_tmdb_id'], df['title'], df['original_title'], df['original_language'], df['overview'], df['poster'], df['cover'] = zip(*df.apply(lambda row: get_images(row['imdb_content_id'], row['type']), axis=1))
    return df


engine = sqlalchemy.create_engine(
    'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
        'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])
conn = engine.connect()

df_titles = pd.read_sql("""
                            select content_id, imdb_content_id, tmdb_id, type
                            from app.content_details
                        """, con=conn)

print('Collecting images...')
df_images = parallelize_dataframe(df_titles, apply_get_images)

language_mapping = {
    'aa': 'Afar',
    'ab': 'Abkhazian',
    'ae': 'Avestan',
    'af': 'Afrikaans',
    'ak': 'Akan',
    'am': 'Amharic',
    'an': 'Aragonese',
    'ar': 'Arabic',
    'as': 'Assamese',
    'av': 'Avaric',
    'ay': 'Aymara',
    'az': 'Azerbaijani',
    'ba': 'Bashkir',
    'be': 'Belarusian',
    'bg': 'Bulgarian',
    'bi': 'Bislama',
    'bm': 'Bambara',
    'bn': 'Bengali',
    'bo': 'Tibetan',
    'br': 'Breton',
    'bs': 'Bosnian',
    'ca': 'Catalan',
    'ce': 'Chechen',
    'ch': 'Chamorro',
    'cn': 'Cantonese',
    'co': 'Corsican',
    'cr': 'Cree',
    'cs': 'Czech',
    'cu': 'Slavic',
    'cv': 'Chuvash',
    'cy': 'Welsh',
    'da': 'Danish',
    'de': 'German',
    'dv': 'Divehi',
    'dz': 'Dzongkha',
    'ee': 'Ewe',
    'el': 'Greek',
    'en': 'English',
    'eo': 'Esperanto',
    'es': 'Spanish',
    'et': 'Estonian',
    'eu': 'Basque',
    'fa': 'Persian',
    'ff': 'Fulah',
    'fi': 'Finnish',
    'fj': 'Fijian',
    'fo': 'Faroese',
    'fr': 'French',
    'fy': 'Frisian',
    'ga': 'Irish',
    'gd': 'Gaelic',
    'gl': 'Galician',
    'gn': 'Guarani',
    'gu': 'Gujarati',
    'gv': 'Manx',
    'ha': 'Hausa',
    'he': 'Hebrew',
    'hi': 'Hindi',
    'ho': 'Hiri Motu',
    'hr': 'Croatian',
    'ht': 'Haitian; Haitian Creole',
    'hu': 'Hungarian',
    'hy': 'Armenian',
    'hz': 'Herero',
    'ia': 'Interlingua',
    'id': 'Indonesian',
    'ie': 'Interlingue',
    'ig': 'Igbo',
    'ii': 'Yi',
    'ik': 'Inupiaq',
    'io': 'Ido',
    'is': 'Icelandic',
    'it': 'Italian',
    'iu': 'Inuktitut',
    'ja': 'Japanese',
    'jv': 'Javanese',
    'ka': 'Georgian',
    'kg': 'Kongo',
    'ki': 'Kikuyu',
    'kj': 'Kuanyama',
    'kk': 'Kazakh',
    'kl': 'Kalaallisut',
    'km': 'Khmer',
    'kn': 'Kannada',
    'ko': 'Korean',
    'kr': 'Kanuri',
    'ks': 'Kashmiri',
    'ku': 'Kurdish',
    'kv': 'Komi',
    'kw': 'Cornish',
    'ky': 'Kirghiz',
    'la': 'Latin',
    'lb': 'Letzeburgesch',
    'lg': 'Ganda',
    'li': 'Limburgish',
    'ln': 'Lingala',
    'lo': 'Lao',
    'lt': 'Lithuanian',
    'lu': 'Luba-Katanga',
    'lv': 'Latvian',
    'mg': 'Malagasy',
    'mh': 'Marshall',
    'mi': 'Maori',
    'mk': 'Macedonian',
    'ml': 'Malayalam',
    'mn': 'Mongolian',
    'mo': 'Moldavian',
    'mr': 'Marathi',
    'ms': 'Malay',
    'mt': 'Maltese',
    'my': 'Burmese',
    'na': 'Nauru',
    'nb': 'Norwegian Bokmål',
    'nd': 'Ndebele',
    'ne': 'Nepali',
    'ng': 'Ndonga',
    'nl': 'Dutch',
    'nn': 'Norwegian Nynorsk',
    'no': 'Norwegian',
    'nr': 'Ndebele',
    'nv': 'Navajo',
    'ny': 'Chichewa; Nyanja',
    'oc': 'Occitan',
    'oj': 'Ojibwa',
    'om': 'Oromo',
    'or': 'Oriya',
    'os': 'Ossetian; Ossetic',
    'pa': 'Punjabi',
    'pi': 'Pali',
    'pl': 'Polish',
    'ps': 'Pushto',
    'pt': 'Portuguese',
    'qu': 'Quechua',
    'rm': 'Raeto-Romance',
    'rn': 'Rundi',
    'ro': 'Romanian',
    'ru': 'Russian',
    'rw': 'Kinyarwanda',
    'sa': 'Sanskrit',
    'sc': 'Sardinian',
    'sd': 'Sindhi',
    'se': 'Northern Sami',
    'sg': 'Sango',
    'sh': 'Serbo-Croatian',
    'si': 'Sinhalese',
    'sk': 'Slovak',
    'sl': 'Slovenian',
    'sm': 'Samoan',
    'sn': 'Shona',
    'so': 'Somali',
    'sq': 'Albanian',
    'sr': 'Serbian',
    'ss': 'Swati',
    'st': 'Sotho',
    'su': 'Sundanese',
    'sv': 'Swedish',
    'sw': 'Swahili',
    'ta': 'Tamil',
    'te': 'Telugu',
    'tg': 'Tajik',
    'th': 'Thai',
    'ti': 'Tigrinya',
    'tk': 'Turkmen',
    'tl': 'Tagalog',
    'tn': 'Tswana',
    'to': 'Tonga',
    'tr': 'Turkish',
    'ts': 'Tsonga',
    'tt': 'Tatar',
    'tw': 'Twi',
    'ty': 'Tahitian',
    'ug': 'Uighur',
    'uk': 'Ukrainian',
    'ur': 'Urdu',
    'uz': 'Uzbek',
    've': 'Venda',
    'vi': 'Vietnamese',
    'vo': 'Volapük',
    'wa': 'Walloon',
    'wo': 'Wolof',
    'xh': 'Xhosa',
    'xx': 'No Language',
    'yi': 'Yiddish',
    'yo': 'Yoruba',
    'za': 'Zhuang',
    'zh': 'Mandarin',
    'zu': 'Zulu'
}
df_images['original_language'] = df_images['original_language'].apply(lambda x: language_mapping.get(x))
df_images.to_csv('images.csv', index=False)
