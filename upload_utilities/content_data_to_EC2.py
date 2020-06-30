import warnings
warnings.filterwarnings("ignore")

import os
import pandas as pd
import yaml
import numpy as np
import re
import sqlalchemy
import sys
import time
from datetime import datetime
from threading import Thread

from utilities import *


config = yaml.safe_load(open('./../config.yml'))
movies_data_folder = config['movies_data_folder']
tv_series_data_folder = config['tv_series_data_folder']
streaming_sources_folder = config['streaming_sources']
to_upload_folder = config['to_upload']
upload_resources_folder = config['upload_resources']
scraped_data_folder = config['scraped_data_folder']


def movies_details():
    df_imdb_details = pd.read_csv(movies_data_folder + 'cleaned_movie_budget_n_metacritic.csv')
    df_tmdb_details = pd.read_csv(movies_data_folder + 'cleaned_movie_tmdb.csv')
    df_synopsis = pd.read_csv(movies_data_folder + 'movie_synopsys.csv')
    df_content = pd.read_csv(movies_data_folder + 'movie_content.csv')
    df_certificates = pd.read_csv(movies_data_folder + 'cleaned_certificates.csv')

    # # exhaustive list of imdb ids
    imdb_ids = list(df_imdb_details['title_id'].unique())
    imdb_ids.extend(list(df_tmdb_details['imdb_id'].unique()))
    imdb_ids.extend(list(df_synopsis['title_id'].unique()))
    imdb_ids.extend(list(df_content['title_id'].unique()))
    imdb_ids.extend(list(df_certificates['title_id'].unique()))
    imdb_ids = list(set(imdb_ids))

    df_movies = pd.DataFrame({'main_imdb_id': imdb_ids})
    df_movies = df_movies[pd.notnull(df_movies['main_imdb_id'])]

    def list_to_dict(items):
        output_dict = {}
        for item in items:
            output_dict.update(item)
        return output_dict

    # # Aggregating content df
    def content_level(item):
        item = str(item)
        if item.lower().strip() == 'mild':
            return 1
        elif item.lower().strip() == 'moderate':
            return 2
        elif item.lower().strip() == 'severe':
            return 3
        else:
            return None


    df_content['level'] = df_content['level'].apply(lambda x: content_level(x))

    df_content.drop_duplicates(inplace=True)

    df_content_pivoted = df_content[['content', 'title_id', 'level']].pivot(index='title_id', columns='content', values='level').reset_index()

    df_content_pivoted.rename(columns = {
        'Sex & Nudity': 'nudity',
        'Violence & Gore': 'violence',
        'Profanity': 'profanity',
        'Alcohol, Drugs & Smoking': 'drugs',
        'Frightening & Intense Scenes': 'intense_scenes'
    }, inplace=True)

    # # Aggregating certificates df
    df_average_age = df_certificates.groupby('title_id').age_limit.mean().reset_index()
    df_average_age['age_limit'] = df_average_age['age_limit'].apply(lambda x: np.ceil(x) if x else x)
    df_mpaa_age = df_certificates[['title_id', 'age_limit']][df_certificates['certificate_by']=='MPAA']

    # # Merging dfs
    df_imdb_details.columns = df_imdb_details.columns.map(lambda x: 'imdb__'+str(x))
    df_movies = pd.merge(df_movies, df_imdb_details, how='left', left_on='main_imdb_id', right_on='imdb__title_id')

    df_tmdb_details.columns = df_tmdb_details.columns.map(lambda x: 'tmdb__'+str(x))
    df_movies = pd.merge(df_movies, df_tmdb_details, how='left', left_on='main_imdb_id', right_on='tmdb__imdb_id')

    df_synopsis.columns = df_synopsis.columns.map(lambda x: 'synopsis__'+str(x))
    df_movies = pd.merge(df_movies, df_synopsis, how='left', left_on='main_imdb_id', right_on='synopsis__title_id')

    df_content_pivoted.columns = df_content_pivoted.columns.map(lambda x: 'content__'+str(x))
    df_movies = pd.merge(df_movies, df_content_pivoted, how='left', left_on='main_imdb_id', right_on='content__title_id')

    df_average_age.columns = df_average_age.columns.map(lambda x: 'average_age__'+str(x))
    df_movies = pd.merge(df_movies, df_average_age, how='left', left_on='main_imdb_id', right_on='average_age__title_id')

    df_mpaa_age.columns = df_mpaa_age.columns.map(lambda x: 'mpaa_age__'+str(x))
    df_movies = pd.merge(df_movies, df_mpaa_age, how='left', left_on='main_imdb_id', right_on='mpaa_age__title_id')

    df_movies = df_movies.where((pd.notnull(df_movies)), None)

    # # Filling release year
    df_movies['imdb__release_year'][pd.notnull(df_movies['imdb__release_year'])] = df_movies['imdb__release_year'][pd.notnull(df_movies['imdb__release_year'])].astype(int)


    def release_year(item):
        year = re.search(r'\d{4}', str(item))
        if year:
            return int(year.group(0))
        else:
            return None


    df_movies['imdb__release_year'][pd.isnull(df_movies['imdb__release_year'])] = df_movies['imdb__release_date'][pd.isnull(df_movies['imdb__release_year'])].apply(lambda x: release_year(x))
    df_movies['imdb__release_year'][pd.isnull(df_movies['imdb__release_year'])] = df_movies['tmdb__release_date'][pd.isnull(df_movies['imdb__release_year'])].apply(lambda x: release_year(x))

    # # Runtime
    df_movies['tmdb__runtime'][pd.notnull(df_movies['tmdb__runtime'])] = df_movies['tmdb__runtime'][pd.notnull(df_movies['tmdb__runtime'])].astype(int)


    def runtime(entry):
        if entry:
            run_time = 0
            items = entry.split(' ')
            for item in items:
                if item.lower().count('h') != 0:
                    run_time += int(item.lower().split('h')[0])*60
                if item.lower().count('min') != 0:
                    run_time += int(item.lower().split('min')[0])
            return run_time
        else:
            return None


    df_movies['tmdb__runtime'][pd.isnull(df_movies['tmdb__runtime'])] = df_movies['imdb__run_time'][pd.isnull(df_movies['tmdb__runtime'])].apply(lambda x: runtime(x))

    # Genre
    df_movies['imdb__genres'] = df_movies['imdb__genres'].apply(lambda x: eval(x)[:3] if x else None)

    # Imdb rating
    df_movies['imdb__imdb_rating'][pd.notnull(df_movies['imdb__imdb_rating'])] = df_movies['imdb__imdb_rating'][pd.notnull(df_movies['imdb__imdb_rating'])].astype(float)

    # Summary
    df_movies['synopsis__summaries'] = df_movies['synopsis__summaries'].apply(lambda x: eval(x)[0] if x else x)

    # Poster
    df_movies['tmdb__poster'][pd.isnull(df_movies['tmdb__poster'])] = df_movies['imdb__title_poster'][pd.isnull(df_movies['tmdb__poster'])]

    # original_language
    def get_language(languages):
        if str(languages).lower() not in ['none', 'nan']:
            return eval(languages)[0].replace('"', '').replace("'", '').strip()
        else:
            return None

    df_movies['tmdb__original_language'][pd.isnull(df_movies['tmdb__original_language'])] = df_movies['imdb__language'][pd.isnull(df_movies['tmdb__original_language'])].apply(get_language)

    # shaping df_movies
    df_movies.rename(columns={
        'main_imdb_id': 'imdb_content_id',
        'tmdb__adult': 'is_adult',
        'average_age__age_limit': 'avg_age_limit',
        'imdb__budget': 'budget',
        'imdb__country': 'country',
        'tmdb__cover_photo': 'cover',
        'tmdb__original_language': 'original_language',
        'imdb__critic_review': 'critic_review',
        'content__drugs': 'drugs',
        'tmdb__facebook': 'facebook',
        'imdb__genres': 'genres',
        'imdb__gross_USA': 'gross_usa',
        'imdb__gross_worldwide': 'gross_worldwide',
        'imdb__imdb_rating': 'imdb_score',
        'imdb__num_votes': 'num_votes',
        'tmdb__instagram': 'instagram',
        'content__intense_scenes': 'intense_scenes',
        'justwatch_ratings__justwatch_rating': 'justwatch_rating',
        'imdb__language': 'language',
        'imdb__filming_location': 'filming_location',
        'imdb__metacritic_score': 'metacritic_score',
        'mpaa_age__age_limit': 'mpaa_age_limit',
        'imdb__award_nominations': 'award_nominations',
        'content__nudity': 'nudity',
        'imdb__opening_weekend_USA': 'opening_weekend_usa',
        'tmdb__poster': 'poster',
        'imdb__production_house': 'production_house',
        'content__profanity': 'profanity',
        'imdb__release_year': 'release_year',
        'justwatch_ratings__rotten_tomatoes_id': 'tomato_id',
        'justwatch_ratings__rotten_tomatoes_rating': 'tomato_score',
        'tmdb__runtime': 'runtime',
        'synopsis__summaries': 'summary_text',
        'imdb__title_name': 'title',
        'tmdb__tmdb_id': 'tmdb_id',
        'tmdb__twitter': 'twitter',
        'imdb__user_review': 'user_review',
        'content__violence': 'violence',
        'tmdb__homepage': 'website',
        'imdb__award_wins': 'award_wins',
        'tmdb__youtube_trailer': 'youtube_trailer_id'
    }, inplace=True)

    basic_columns = ['content_id', 'imdb_content_id', 'title', 'original_title', 'is_adult', 'release_year', 'runtime', 'genres', 'imdb_score', 'num_votes', 'summary_text', 'country', 'language', 'filming_location', 'production_house', 'budget', 'opening_weekend_usa', 'gross_usa', 'gross_worldwide', 'critic_review', 'user_review', 'award_wins', 'award_nominations', 'youtube_trailer_id', 'cover', 'poster', 'metacritic_score', 'tmdb_id', 'tmdb_popularity', 'tmdb_score', 'tomato_id', 'tomato_meter', 'tomato_rating', 'tomato_score', 'tomato_userrating_meter', 'tomato_userrating_rating', 'tomato_userrating_score', 'nudity', 'violence', 'profanity', 'drugs', 'intense_scenes', 'avg_age_limit', 'mpaa_age_limit', 'tags', 'similar_content', 'filtered_content', 'justwatch_rating', 'original_language']
    df_columns = list(df_movies.columns)
    missing_columns = [col for col in basic_columns if col not in df_columns]
    for col in missing_columns:
        df_movies[col] = None

    df_movies = df_movies[pd.notnull(df_movies['title']) & pd.notnull(df_movies['runtime']) &
                          pd.notnull(df_movies['imdb_score']) & pd.notnull(df_movies['num_votes'])]
    df_movies.sort_values('num_votes', ascending=False, inplace=True)

    int_columns = ['release_year', 'runtime', 'num_votes', 'critic_review', 'user_review', 'award_wins', 'award_nominations',
                   'metacritic_score', 'tmdb_id', 'tomato_id', 'tomato_meter', 'tomato_rating', 'tomato_score',
                   'tomato_userrating_meter', 'tomato_userrating_rating', 'tomato_userrating_score', 'nudity', 'violence',
                   'profanity', 'drugs', 'intense_scenes', 'avg_age_limit', 'mpaa_age_limit']
    for col in int_columns:
        df_movies[col][pd.notnull(df_movies[col])] = df_movies[col][pd.notnull(df_movies[col])].apply(lambda x: eval(str(x).replace(',', '')))
        df_movies[col][pd.notnull(df_movies[col])] = df_movies[col][pd.notnull(df_movies[col])].apply(lambda x: '{:.0f}'.format(x))

    bool_columns = ['is_adult']
    for col in bool_columns:
        df_movies[col] = df_movies[col].astype(bool)
        df_movies[col][pd.notnull(df_movies[col])] = df_movies[col][pd.notnull(df_movies[col])].apply(lambda x: str(x).lower())

    def clean_col_array(str_array):
        cleaned_array = []
        for item in eval(str_array):
            cleaned_array.append(item.replace('"', '').replace("'", '').strip())
        return set(cleaned_array)
    list_columns = ['genres', 'country', 'language', 'production_house']
    for col in list_columns:
        df_movies[col][pd.notnull(df_movies[col])] = df_movies[col][pd.notnull(df_movies[col])].apply(lambda x: clean_col_array(str(x)))

    df_movies = df_movies[['content_id', 'imdb_content_id', 'title', 'original_title', 'is_adult', 'release_year', 'runtime', 'genres',
                           'imdb_score', 'num_votes', 'summary_text', 'country', 'language', 'filming_location', 'production_house',
                           'budget', 'opening_weekend_usa', 'gross_usa', 'gross_worldwide', 'critic_review', 'user_review',
                           'award_wins', 'award_nominations', 'youtube_trailer_id', 'cover', 'poster', 'metacritic_score',
                           'tmdb_id', 'tmdb_popularity', 'tmdb_score', 'tomato_id', 'tomato_meter', 'tomato_rating', 'tomato_score',
                           'tomato_userrating_meter', 'tomato_userrating_rating', 'tomato_userrating_score',
                           'nudity', 'violence', 'profanity', 'drugs', 'intense_scenes', 'avg_age_limit', 'mpaa_age_limit', 'tags',
                           'similar_content', 'filtered_content', 'justwatch_rating', 'website', 'facebook','instagram', 'twitter',
                           'original_language']]

    # Creating content_id based on ids in db
    engine = sqlalchemy.create_engine('postgres://'+config['sql']['user']+':'+config['sql']['password']+'@'+config['sql']['host']+':'+str(config['sql']['port'])+'/'+config['sql']['db'])
    conn = engine.connect()

    df_db_movies = pd.read_sql("""
                               select content_id, imdb_content_id
                               from """+config['sql']['schema']+""".content_details
                               where type = 'movie'
                               """, con=conn)

    max_content_id = df_db_movies['content_id'].apply(lambda x: int(str(x)[1:])).max()
    if str(max_content_id) == 'nan':
        max_content_id = 0

    backed_up_movies = list(df_db_movies['imdb_content_id'].unique())
    df_movies = df_movies[~(df_movies.imdb_content_id.isin(backed_up_movies))]

    df_movies.reset_index(inplace=True, drop=True)

    df_movies['content_id'] = df_movies.index+max_content_id+1
    df_movies['content_id'] = df_movies['content_id'].apply(lambda x: int('1'+str(x)))

    conn.close()

    df_movies.to_csv(to_upload_folder+'movies.csv', sep='^', index=False)

    upload_command = 'scp -i '+config['pem_key']+' '+to_upload_folder+'movies.csv '+config['ec2']['user']+'@'+config['ec2']['public_dns']+':'+config['ec2']['file_upload_location']
    os.system(upload_command)

    return True


def tv_series_details():
    df_imdb_details = pd.read_csv(tv_series_data_folder + 'cleaned_tv_series_details.csv')
    df_tmdb_details = pd.read_csv(tv_series_data_folder + 'cleaned_tv_series_tmdb.csv')
    df_synopsis = pd.read_csv(tv_series_data_folder + 'tv_series_synopsys.csv')
    df_content = pd.read_csv(tv_series_data_folder + 'tv_series_content.csv')
    df_certificates = pd.read_csv(tv_series_data_folder + 'cleaned_certificates.csv')

    # # exhaustive list of imdb ids
    imdb_ids = list(df_imdb_details['title_id'].unique())
    imdb_ids.extend(list(df_tmdb_details['imdb_id'].unique()))
    imdb_ids.extend(list(df_synopsis['title_id'].unique()))
    imdb_ids.extend(list(df_content['title_id'].unique()))
    imdb_ids.extend(list(df_certificates['title_id'].unique()))
    imdb_ids = list(set(imdb_ids))

    df_tv_series = pd.DataFrame({'main_imdb_id': imdb_ids})
    df_tv_series = df_tv_series[pd.notnull(df_tv_series['main_imdb_id'])]

    def list_to_dict(items):
        output_dict = {}
        for item in items:
            output_dict.update(item)
        return output_dict

    # # Aggregating content df
    def content_level(item):
        item = str(item)
        if item.lower().strip() == 'mild':
            return 1
        elif item.lower().strip() == 'moderate':
            return 2
        elif item.lower().strip() == 'severe':
            return 3
        else:
            return None


    df_content['level'] = df_content['level'].apply(lambda x: content_level(x))

    df_content.drop_duplicates(inplace=True)

    df_content_pivoted = df_content[['content', 'title_id', 'level']].pivot(index='title_id', columns='content', values='level').reset_index()

    df_content_pivoted.rename(columns = {
        'Sex & Nudity': 'nudity',
        'Violence & Gore': 'violence',
        'Profanity': 'profanity',
        'Alcohol, Drugs & Smoking': 'drugs',
        'Frightening & Intense Scenes': 'intense_scenes'
    }, inplace=True)

    # # Aggregating certificates df
    df_average_age = df_certificates.groupby('title_id').age_limit.mean().reset_index()
    df_average_age['age_limit'] = df_average_age['age_limit'].apply(lambda x: np.ceil(x) if x else x)
    df_mpaa_age = df_certificates[['title_id', 'age_limit']][df_certificates['certificate_by']=='MPAA']

    # # Merging dfs
    df_imdb_details.columns = df_imdb_details.columns.map(lambda x: 'imdb__'+str(x))
    df_tv_series = pd.merge(df_tv_series, df_imdb_details, how='left', left_on='main_imdb_id', right_on='imdb__title_id')

    df_tmdb_details.columns = df_tmdb_details.columns.map(lambda x: 'tmdb__'+str(x))
    df_tv_series = pd.merge(df_tv_series, df_tmdb_details, how='left', left_on='main_imdb_id', right_on='tmdb__imdb_id')

    df_synopsis.columns = df_synopsis.columns.map(lambda x: 'synopsis__'+str(x))
    df_tv_series = pd.merge(df_tv_series, df_synopsis, how='left', left_on='main_imdb_id', right_on='synopsis__title_id')

    df_content_pivoted.columns = df_content_pivoted.columns.map(lambda x: 'content__'+str(x))
    df_tv_series = pd.merge(df_tv_series, df_content_pivoted, how='left', left_on='main_imdb_id', right_on='content__title_id')

    df_average_age.columns = df_average_age.columns.map(lambda x: 'average_age__'+str(x))
    df_tv_series = pd.merge(df_tv_series, df_average_age, how='left', left_on='main_imdb_id', right_on='average_age__title_id')

    df_mpaa_age.columns = df_mpaa_age.columns.map(lambda x: 'mpaa_age__'+str(x))
    df_tv_series = pd.merge(df_tv_series, df_mpaa_age, how='left', left_on='main_imdb_id', right_on='mpaa_age__title_id')

    df_tv_series = df_tv_series.where((pd.notnull(df_tv_series)), None)

    # Filling release & end year
    def production_year(item):
        item = str(item).replace('â€“', '–')
        items = item.split('–')
        if len(items) == 1:
            items.append('None')
        years = []
        for item in items:
            year = re.search(r'^\d{4}$', str(item))
            if year:
                years.append(int(year.group(0)))
            else:
                years.append(None)

        return years[0], years[1]

    df_tv_series['release_year'], df_tv_series['end_year'] = zip(*df_tv_series['imdb__time_period'].apply(production_year))

    df_tv_series['release_year'][pd.isnull(df_tv_series['release_year'])] = df_tv_series['tmdb__first_air_date'][pd.isnull(df_tv_series['release_year'])].apply(lambda x: pd.to_datetime(x).year if x else None)
    df_tv_series['end_year'][pd.isnull(df_tv_series['end_year']) & (df_tv_series['tmdb__in_production'] is False)] = df_tv_series['tmdb__last_air_date'][pd.isnull(df_tv_series['end_year']) & (df_tv_series['tmdb__in_production'] is False)].apply(lambda x: pd.to_datetime(x).year if x else None)

    # # Runtime
    df_tv_series['tmdb__episode_runtime'][pd.notnull(df_tv_series['tmdb__episode_runtime'])] = df_tv_series['tmdb__episode_runtime'][pd.notnull(df_tv_series['tmdb__episode_runtime'])].astype(int)


    def runtime(entry):
        if entry:
            run_time = 0
            items = entry.split(' ')
            for item in items:
                if item.lower().count('h') != 0:
                    run_time += int(item.lower().split('h')[0])*60
                if item.lower().count('min') != 0:
                    run_time += int(item.lower().split('min')[0])
            return run_time
        else:
            return None


    df_tv_series['tmdb__episode_runtime'][pd.isnull(df_tv_series['tmdb__episode_runtime'])] = df_tv_series['imdb__avg_run_time'][pd.isnull(df_tv_series['tmdb__episode_runtime'])].apply(lambda x: runtime(x))

    # Genre
    df_tv_series['imdb__genres'] = df_tv_series['imdb__genres'].apply(lambda x: eval(x)[:3] if x else None)

    # Imdb rating
    df_tv_series['imdb__imdb_rating'][pd.notnull(df_tv_series['imdb__imdb_rating'])] = df_tv_series['imdb__imdb_rating'][pd.notnull(df_tv_series['imdb__imdb_rating'])].astype(float)

    # Summary
    df_tv_series['synopsis__summaries'] = df_tv_series['synopsis__summaries'].apply(lambda x: eval(x)[0] if x else x)

    # Poster
    df_tv_series['tmdb__poster'][pd.isnull(df_tv_series['tmdb__poster'])] = df_tv_series['imdb__title_poster'][pd.isnull(df_tv_series['tmdb__poster'])]

    # original_language
    def get_language(languages):
        if str(languages).lower() not in ['none', 'nan']:
            return eval(languages)[0].replace('"', '').replace("'", '').strip()
        else:
            return None

    df_tv_series['tmdb__original_language'][pd.isnull(df_tv_series['tmdb__original_language'])] = df_tv_series['imdb__language'][
        pd.isnull(df_tv_series['tmdb__original_language'])].apply(get_language)

    # shaping df_tv_series
    df_tv_series.rename(columns={
        'main_imdb_id': 'imdb_content_id',
        'tmdb__adult': 'is_adult',
        'average_age__age_limit': 'avg_age_limit',
        'imdb__budget': 'budget',
        'imdb__country': 'country',
        'tmdb__cover_photo': 'cover',
        'imdb__critic_review': 'critic_review',
        'tmdb__original_language': 'original_language',
        'content__drugs': 'drugs',
        'tmdb__facebook': 'facebook',
        'imdb__genres': 'genres',
        'imdb__gross_USA': 'gross_usa',
        'imdb__gross_worldwide': 'gross_worldwide',
        'imdb__imdb_rating': 'imdb_score',
        'imdb__num_votes': 'num_votes',
        'tmdb__instagram': 'instagram',
        'content__intense_scenes': 'intense_scenes',
        'justwatch_ratings__justwatch_rating': 'justwatch_rating',
        'imdb__language': 'language',
        'imdb__filming_location': 'filming_location',
        'imdb__metacritic_score': 'metacritic_score',
        'mpaa_age__age_limit': 'mpaa_age_limit',
        'imdb__award_nominations': 'award_nominations',
        'content__nudity': 'nudity',
        'imdb__opening_weekend_USA': 'opening_weekend_usa',
        'tmdb__poster': 'poster',
        'imdb__production_house': 'production_house',
        'content__profanity': 'profanity',
        'imdb__release_year': 'release_year',
        'imdb__end_year': 'end_year',
        'justwatch_ratings__rotten_tomatoes_id': 'tomato_id',
        'justwatch_ratings__rotten_tomatoes_rating': 'tomato_score',
        'tmdb__episode_runtime': 'runtime',
        'synopsis__summaries': 'summary_text',
        'imdb__title_name': 'title',
        'tmdb__tmdb_id': 'tmdb_id',
        'tmdb__twitter': 'twitter',
        'imdb__user_review': 'user_review',
        'content__violence': 'violence',
        'tmdb__homepage': 'website',
        'imdb__award_wins': 'award_wins',
        'tmdb__youtube_trailer': 'youtube_trailer_id',
        'tmdb__number_of_episodes': 'episodes',
        'tmdb__number_of_seasons': 'seasons',
        'tmdb__in_production': 'in_production',
        'tmdb__content_type': 'scripting'
    }, inplace=True)

    basic_columns = ['content_id', 'imdb_content_id', 'title', 'original_title', 'is_adult', 'release_year', 'end_year', 'runtime', 'genres', 'imdb_score', 'num_votes', 'summary_text', 'country', 'language', 'filming_location', 'production_house', 'budget', 'opening_weekend_usa', 'gross_usa', 'gross_worldwide', 'critic_review', 'user_review', 'award_wins', 'award_nominations', 'youtube_trailer_id', 'cover', 'poster', 'metacritic_score', 'tmdb_id', 'tmdb_popularity', 'tmdb_score', 'tomato_id', 'tomato_meter', 'tomato_rating', 'tomato_score', 'tomato_userrating_meter', 'tomato_userrating_rating', 'tomato_userrating_score', 'nudity', 'violence', 'profanity', 'drugs', 'intense_scenes', 'avg_age_limit', 'mpaa_age_limit', 'tags', 'similar_content', 'filtered_content', 'episodes', 'seasons', 'in_production', 'scripting', 'justwatch_rating', 'original_language']
    df_columns = list(df_tv_series.columns)
    missing_columns = [col for col in basic_columns if col not in df_columns]
    for col in missing_columns:
        df_tv_series[col] = None

    df_tv_series = df_tv_series[pd.notnull(df_tv_series['title']) & pd.notnull(df_tv_series['runtime']) &
                          pd.notnull(df_tv_series['imdb_score']) & pd.notnull(df_tv_series['num_votes'])]
    df_tv_series.sort_values('num_votes', ascending=False, inplace=True)

    int_columns = ['release_year', 'end_year', 'runtime', 'num_votes', 'critic_review', 'user_review', 'award_wins', 'award_nominations',
                   'metacritic_score', 'tmdb_id', 'tomato_id', 'tomato_meter', 'tomato_rating', 'tomato_score',
                   'tomato_userrating_meter', 'tomato_userrating_rating', 'tomato_userrating_score', 'nudity', 'violence',
                   'profanity', 'drugs', 'intense_scenes', 'avg_age_limit', 'mpaa_age_limit', 'episodes', 'seasons']
    for col in int_columns:
        df_tv_series[col][pd.notnull(df_tv_series[col])] = df_tv_series[col][pd.notnull(df_tv_series[col])].apply(lambda x: eval(str(x).replace(',', '')))
        df_tv_series[col][pd.notnull(df_tv_series[col])] = df_tv_series[col][pd.notnull(df_tv_series[col])].apply(lambda x: '{:.0f}'.format(x))

    bool_columns = ['is_adult', 'in_production']
    for col in bool_columns:
        df_tv_series[col] = df_tv_series[col].astype(bool)
        df_tv_series[col][pd.notnull(df_tv_series[col])] = df_tv_series[col][pd.notnull(df_tv_series[col])].apply(lambda x: str(x).lower())

    def clean_col_array(str_array):
        cleaned_array = []
        for item in eval(str_array):
            cleaned_array.append(item.replace('"', '').replace("'", '').strip())
        return set(cleaned_array)
    list_columns = ['genres', 'country', 'language', 'production_house']
    for col in list_columns:
        df_tv_series[col][pd.notnull(df_tv_series[col])] = df_tv_series[col][pd.notnull(df_tv_series[col])].apply(lambda x: clean_col_array(str(x)))

    df_tv_series = df_tv_series[['content_id', 'imdb_content_id', 'title', 'original_title', 'is_adult', 'release_year', 'end_year', 'runtime', 'genres',
                           'imdb_score', 'num_votes', 'summary_text', 'country', 'language', 'filming_location', 'production_house',
                           'budget', 'opening_weekend_usa', 'gross_usa', 'gross_worldwide', 'critic_review', 'user_review',
                           'award_wins', 'award_nominations', 'youtube_trailer_id', 'cover', 'poster', 'metacritic_score',
                           'tmdb_id', 'tmdb_popularity', 'tmdb_score', 'tomato_id', 'tomato_meter', 'tomato_rating', 'tomato_score',
                           'tomato_userrating_meter', 'tomato_userrating_rating', 'tomato_userrating_score',
                           'nudity', 'violence', 'profanity', 'drugs', 'intense_scenes', 'avg_age_limit', 'mpaa_age_limit', 'tags',
                           'similar_content', 'filtered_content', 'justwatch_rating', 'website', 'facebook','instagram', 'twitter',
                           'episodes', 'seasons', 'in_production', 'scripting', 'original_language']]
    df_tv_series['type'] = 'tv'

    # Creating content_id based on ids in db
    engine = sqlalchemy.create_engine('postgres://'+config['sql']['user']+':'+config['sql']['password']+'@'+config['sql']['host']+':'+str(config['sql']['port'])+'/'+config['sql']['db'])
    conn = engine.connect()

    df_db_tv_series = pd.read_sql("""
                               select content_id, imdb_content_id
                               from """+config['sql']['schema']+""".content_details
                               where type = 'tv'
                               """, con=conn)

    max_content_id = df_db_tv_series['content_id'].apply(lambda x: int(str(x)[1:])).max()
    if str(max_content_id) == 'nan':
        max_content_id = 0

    backed_up_tv_series = list(df_db_tv_series['imdb_content_id'].unique())
    df_tv_series = df_tv_series[~(df_tv_series.imdb_content_id.isin(backed_up_tv_series))]

    df_tv_series.reset_index(inplace=True, drop=True)

    df_tv_series['content_id'] = df_tv_series.index+max_content_id+1
    df_tv_series['content_id'] = df_tv_series['content_id'].apply(lambda x: int('2'+str(x)))

    conn.close()

    df_tv_series.to_csv(to_upload_folder+'tv_series.csv', sep='^', index=False)

    upload_command = 'scp -i '+config['pem_key']+' '+to_upload_folder+'tv_series.csv '+config['ec2']['user']+'@'+config['ec2']['public_dns']+':'+config['ec2']['file_upload_location']
    os.system(upload_command)

    return True


def artists():
    df_movies_crew = pd.read_csv(movies_data_folder+'movie_crew.csv')
    df_movies_tmdb = pd.read_csv(movies_data_folder+'movie_tmdb_artists.csv')

    df_tv_series_crew = pd.read_csv(tv_series_data_folder + 'cleaned_tv_series_crew.csv')
    df_tv_series_tmdb = pd.read_csv(tv_series_data_folder + 'tv_series_tmdb_artists.csv')

    df_artists = pd.concat([df_movies_crew[['person_id', 'person']],
                            df_tv_series_crew[['person_id', 'person']]], axis=0)
    df_artists.drop_duplicates(inplace=True)

    total_rows = df_artists.shape[0]
    total_artists = df_artists['person_id'].unique().shape[0]
    if total_rows != total_artists:
        print('Duplicate artist entries found, removing duplicates...')
        print('Total rows -', total_rows, '| Total artists -', total_artists)

        df_artists.drop_duplicates(subset='person_id', keep='first', inplace=True)

        total_rows = df_artists.shape[0]
        total_artists = df_artists['person_id'].unique().shape[0]
        print('\nAfter removing the duplicates...')
        print('Total rows -', total_rows, '| Total artists -', total_artists)

    df_tmdb = pd.concat([df_movies_tmdb[['imdb_id', 'name', 'profile_path']],
                         df_tv_series_tmdb[['imdb_id', 'name', 'profile_path']]], axis=0)
    df_tmdb.drop_duplicates(inplace=True)

    df_artists = pd.merge(df_artists, df_tmdb, how='left', left_on='person_id', right_on='imdb_id')

    df_artists['person'][pd.isnull(df_artists['person']) | (df_artists['person']=='')] = df_artists['name'][pd.isnull(df_artists['person']) | (df_artists['person']=='')]

    del df_artists['name']
    del df_artists['imdb_id']

    tmdb_image_base_url = 'https://image.tmdb.org/t/p/w500'
    df_artists['profile_path'] = df_artists['profile_path'].apply(lambda x: tmdb_image_base_url + x if str(x).lower() not in ['nan', 'none'] else None)

    df_artists.rename(columns={
        'person_id': 'imdb_person_id',
        'person': 'name',
        'profile_path': 'picture'
    }, inplace=True)

    # Creating person_id based on ids in db
    engine = sqlalchemy.create_engine(
        'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
            'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])
    conn = engine.connect()

    df_db_artists = pd.read_sql("""
                                   select person_id, imdb_person_id
                                   from """+config['sql']['schema']+""".artists
                                   """, con=conn)

    max_person_id = df_db_artists['person_id'].max()
    if str(max_person_id) == 'nan':
        max_person_id = 0

    backed_up_artists = list(df_db_artists['imdb_person_id'].unique())
    df_artists = df_artists[~(df_artists.imdb_person_id.isin(backed_up_artists))]

    df_artists.sort_values('picture', ascending=False, inplace=True)
    df_artists.reset_index(inplace=True, drop=True)

    df_artists['person_id'] = df_artists.index + max_person_id + 1

    conn.close()

    df_artists.to_csv(to_upload_folder + 'artists.csv', sep='^', index=False)

    upload_command = 'scp -i ' + config['pem_key'] + ' ' + to_upload_folder + 'artists.csv ' + config['ec2'][
        'user'] + '@' + config['ec2']['public_dns'] + ':' + config['ec2']['file_upload_location']
    os.system(upload_command)

    return True


def content_crew():
    df_movies_crew = pd.read_csv(movies_data_folder+'movie_crew.csv')
    df_tv_series_crew = pd.read_csv(tv_series_data_folder + 'cleaned_tv_series_crew.csv')
    df_crew = pd.concat([df_movies_crew, df_tv_series_crew], axis=0)
    del df_crew['title_name']
    del df_crew['person']

    try:
        df_movies = pd.read_csv(to_upload_folder+'movies.csv', sep='^')
        df_movies = df_movies[['content_id', 'imdb_content_id']]
    except:
        df_movies = pd.DataFrame(columns=['content_id', 'imdb_content_id'])

    try:
        df_tv_series = pd.read_csv(to_upload_folder + 'tv_series.csv', sep='^')
        df_tv_series = df_tv_series[['content_id', 'imdb_content_id']]
    except:
        df_tv_series = pd.DataFrame(columns=['content_id', 'imdb_content_id'])

    df_content = pd.concat([df_movies, df_tv_series], axis=0)

    df_artists = pd.read_csv(to_upload_folder + 'artists.csv', sep='^')
    df_artists = df_artists[['person_id', 'imdb_person_id']]

    engine = sqlalchemy.create_engine(
        'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
            'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])

    conn = engine.connect()
    df_db_content = pd.read_sql("""
                               select content_id, imdb_content_id
                               from """+config['sql']['schema']+""".content_details
                               """, con=conn)
    df_db_artists = pd.read_sql("""
                               select person_id, imdb_person_id
                               from """+config['sql']['schema']+""".artists
                               """, con=conn)

    backed_up_content = list(df_db_content['imdb_content_id'].unique())
    df_crew = df_crew[~(df_crew.title_id.isin(backed_up_content))]

    df_artists = pd.concat([df_db_artists,df_artists], axis=0)
    df_artists.drop_duplicates(inplace=True)

    df_crew = pd.merge(df_crew, df_content, how='left', left_on='title_id', right_on='imdb_content_id')
    del df_crew['imdb_content_id']
    del df_crew['title_id']

    df_crew.rename(columns={'person_id': 'imdb_artist_id'}, inplace=True)
    df_crew = pd.merge(df_crew, df_artists, how='left', left_on='imdb_artist_id', right_on='imdb_person_id')
    del df_crew['imdb_person_id']
    del df_crew['imdb_artist_id']

    df_crew = df_crew[pd.notnull(df_crew['content_id'])]

    def credit_years(item):
        item = str(item).replace('â€“', '–').replace('-', '–')
        items = item.split('–')
        if len(items) == 1:
            items.append('None')
        years = []
        for item in items:
            year = re.search(r'^\d{4}$', str(item))
            if year:
                years.append(int(year.group(0)))
            else:
                years.append(None)

        return years[0], years[1]

    df_crew['credit_start_year'], df_crew['credit_end_year'] = zip(*df_crew['credit_time_period'].apply(credit_years))
    del df_crew['credit_time_period']

    int_columns = ['person_id', 'content_id', 'credit_order', 'credit_episodes', 'credit_start_year', 'credit_end_year']
    for col in int_columns:
        df_crew[col][pd.notnull(df_crew[col])] = df_crew[col][pd.notnull(df_crew[col])].apply(
            lambda x: eval(str(x).replace(',', '')))
        df_crew[col][pd.notnull(df_crew[col])] = df_crew[col][pd.notnull(df_crew[col])].apply(
            lambda x: '{:.0f}'.format(x))

    df_crew[['person_id', 'content_id', 'credit_as', 'credit_category', 'credit_order']].to_csv(to_upload_folder + 'content_crew.csv', sep='^', index=False)

    conn.close()

    upload_command = 'scp -i ' + config['pem_key'] + ' ' + to_upload_folder + 'content_crew.csv ' + config['ec2'][
        'user'] + '@' + config['ec2']['public_dns'] + ':' + config['ec2']['file_upload_location']
    os.system(upload_command)

    return True


def content_tags():
    df_movie_tags = pd.read_csv(movies_data_folder+'cleaned_movie_keywords.csv')
    df_tv_series_tags = pd.read_csv(tv_series_data_folder + 'cleaned_tv_series_keywords.csv')
    df_tags = pd.concat([df_movie_tags, df_tv_series_tags], axis=0)
    del df_tags['title_name']

    try:
        df_movies = pd.read_csv(to_upload_folder + 'movies.csv', sep='^')
    except:
        df_movies = pd.DataFrame(columns=['content_id', 'imdb_content_id'])
    try:
        df_tv_series = pd.read_csv(to_upload_folder + 'tv_series.csv', sep='^')
    except:
        df_tv_series = pd.DataFrame(columns=['content_id', 'imdb_content_id'])
    df_content = pd.concat([df_movies[['content_id', 'imdb_content_id']],
                            df_tv_series[['content_id', 'imdb_content_id']]], axis=0)

    engine = sqlalchemy.create_engine(
        'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
            'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])

    conn = engine.connect()
    df_db_content = pd.read_sql("""
                               select content_id, imdb_content_id
                               from """+config['sql']['schema']+""".content_details
                               """, con=conn)

    backed_up_content = list(df_db_content['imdb_content_id'].unique())
    df_tags = df_tags[~(df_tags.title_id.isin(backed_up_content))]

    df_tags = pd.merge(df_tags, df_content, how='left', left_on='title_id', right_on='imdb_content_id')
    del df_tags['imdb_content_id']
    del df_tags['title_id']

    df_tags.rename(columns={
        'keyword': 'tag',
        'order': 'tag_order'
    }, inplace=True)

    df_tags = df_tags[pd.notnull(df_tags['content_id'])]

    int_columns = ['content_id', 'tag_order', 'total_votes', 'upvotes']
    for col in int_columns:
        df_tags[col][pd.notnull(df_tags[col])] = df_tags[col][pd.notnull(df_tags[col])].apply(
            lambda x: eval(str(x).replace(',', '')))
        df_tags[col][pd.notnull(df_tags[col])] = df_tags[col][pd.notnull(df_tags[col])].apply(
            lambda x: '{:.0f}'.format(x))

        df_tags[['content_id', 'tag', 'tag_order', 'total_votes', 'upvotes']].to_csv(to_upload_folder + 'content_tags.csv', sep='^', index=False)

    conn.close()

    upload_command = 'scp -i ' + config['pem_key'] + ' ' + to_upload_folder + 'content_tags.csv ' + config['ec2'][
        'user'] + '@' + config['ec2']['public_dns'] + ':' + config['ec2']['file_upload_location']
    os.system(upload_command)

    return True


def awards_master():
    upload_command = 'scp -i ' + config['pem_key'] + ' ' + to_upload_folder + 'awards_master.csv ' + config['ec2'][
        'user'] + '@' + config['ec2']['public_dns'] + ':' + config['ec2']['file_upload_location']
    os.system(upload_command)

    return True


def content_certificates():
    df_movies_certis = pd.read_csv(movies_data_folder+'cleaned_certificates.csv')
    df_tv_series_certis = pd.read_csv(tv_series_data_folder + 'cleaned_certificates.csv')

    df_certis = pd.concat([df_movies_certis, df_tv_series_certis], axis=0)

    try:
        df_movies = pd.read_csv(to_upload_folder + 'movies.csv', sep='^')
    except:
        df_movies = pd.DataFrame(columns=['content_id', 'imdb_content_id'])
    try:
        df_tv_series = pd.read_csv(to_upload_folder + 'tv_series.csv', sep='^')
    except:
        df_tv_series = pd.DataFrame(columns=['content_id', 'imdb_content_id'])
    df_content = pd.concat([df_movies[['content_id', 'imdb_content_id']],
                            df_tv_series[['content_id', 'imdb_content_id']]], axis=0)

    engine = sqlalchemy.create_engine(
        'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
            'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])

    conn = engine.connect()
    df_db_content = pd.read_sql("""
                                   select content_id, imdb_content_id
                                   from """ + config['sql']['schema'] + """.content_details
                                   """, con=conn)

    backed_up_content = list(df_db_content['imdb_content_id'].unique())
    df_certis = df_certis[~(df_certis.title_id.isin(backed_up_content))]

    df_certis = pd.merge(df_certis, df_content, how='left', left_on='title_id', right_on='imdb_content_id')
    del df_certis['imdb_content_id']
    del df_certis['title_id']

    df_certis.rename(columns={
        'pg': 'parental_guide'
    }, inplace=True)

    df_certis = df_certis[pd.notnull(df_certis['content_id'])]

    int_columns = ['age_limit', 'content_id']
    for col in int_columns:
        df_certis[col][pd.notnull(df_certis[col])] = df_certis[col][pd.notnull(df_certis[col])].apply(
            lambda x: eval(str(x).replace(',', '')))
        df_certis[col][pd.notnull(df_certis[col])] = df_certis[col][pd.notnull(df_certis[col])].apply(
            lambda x: '{:.0f}'.format(x))

    bool_columns = ['parental_guide', 'banned']
    for col in bool_columns:
        df_certis[col] = df_certis[col].astype(bool)
        df_certis[col][pd.notnull(df_certis[col])] = df_certis[col][pd.notnull(df_certis[col])].apply(
            lambda x: str(x).lower())

    df_certis[['content_id', 'certificate_by', 'rating', 'rating_cleaned', 'age_limit', 'parental_guide', 'banned']].to_csv(
        to_upload_folder + 'content_certificates.csv', sep='^', index=False)

    conn.close()

    upload_command = 'scp -i ' + config['pem_key'] + ' ' + to_upload_folder + 'content_certificates.csv ' + config['ec2'][
        'user'] + '@' + config['ec2']['public_dns'] + ':' + config['ec2']['file_upload_location']
    os.system(upload_command)

    return True


def awards_distribution():
    df_awards_distribution = pd.read_csv(scraped_data_folder+'cleaned_awards_scraped.csv')

    try:
        df_movies = pd.read_csv(to_upload_folder + 'movies.csv', sep='^')
    except:
        df_movies = pd.DataFrame(columns=['content_id', 'imdb_content_id'])
    try:
        df_tv_series = pd.read_csv(to_upload_folder + 'tv_series.csv', sep='^')
    except:
        df_tv_series = pd.DataFrame(columns=['content_id', 'imdb_content_id'])
    df_content = pd.concat([df_movies[['content_id', 'imdb_content_id']],
                            df_tv_series[['content_id', 'imdb_content_id']]], axis=0)

    try:
        df_artists = pd.read_csv(to_upload_folder + 'artists.csv', sep='^')
        df_artists = df_artists[['person_id', 'imdb_person_id']]
    except:
        df_artists = pd.DataFrame(columns=['person_id', 'imdb_person_id'])

    engine = sqlalchemy.create_engine(
        'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
            'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])

    conn = engine.connect()
    df_db_content = pd.read_sql("""
                                   select content_id, imdb_content_id
                                   from """ + config['sql']['schema'] + """.content_details
                                   """, con=conn)
    df_db_artists = pd.read_sql("""
                                   select person_id, imdb_person_id
                                   from """ + config['sql']['schema'] + """.artists
                                   """, con=conn)

    df_content = pd.concat([df_db_content, df_content], axis=0)
    df_content.drop_duplicates(inplace=True)
    df_artists = pd.concat([df_db_artists, df_artists], axis=0)
    df_artists.drop_duplicates(inplace=True)

    df_awards_distribution = pd.merge(df_awards_distribution, df_content, how='left', on='imdb_content_id')
    del df_awards_distribution['imdb_content_id']

    df_awards_distribution = pd.merge(df_awards_distribution, df_artists, how='left', on='imdb_person_id')
    del df_awards_distribution['imdb_person_id']

    df_awards_distribution = df_awards_distribution.where((pd.notnull(df_awards_distribution)), None)
    df_awards_distribution = df_awards_distribution[(pd.notnull(df_awards_distribution['content_id'])) |
                                                    (pd.notnull(df_awards_distribution['person_id']))]

    int_columns = ['content_id', 'person_id', 'award_id', 'event_year']
    for col in int_columns:
        df_awards_distribution[col][pd.notnull(df_awards_distribution[col])] = df_awards_distribution[col][
            pd.notnull(df_awards_distribution[col])].apply(
            lambda x: eval(str(x).replace(',', '')))
        df_awards_distribution[col][pd.notnull(df_awards_distribution[col])] = df_awards_distribution[col][
            pd.notnull(df_awards_distribution[col])].apply(
            lambda x: '{:.0f}'.format(x))

    bool_columns = ['won']
    for col in bool_columns:
        df_awards_distribution[col] = df_awards_distribution[col].astype(bool)
        df_awards_distribution[col][pd.notnull(df_awards_distribution[col])] = df_awards_distribution[col][
            pd.notnull(df_awards_distribution[col])].apply(
            lambda x: str(x).lower())

    df_db_awards = pd.read_sql("""
                                       select award_id, event_year, content_id, person_id
                                       from """ + config['sql']['schema'] + """.awards_distribution
                                       """, con=conn)
    df_db_awards = df_db_awards.where((pd.notnull(df_db_awards)), None)
    if not df_db_awards.empty:
        df_db_awards['db_awards'] = df_db_awards.apply(lambda row: str(row['award_id'])+ '^' +
                                                                   str(row['event_year']) + '^' +
                                                                   str(row['content_id']) + '^' +
                                                                   str(row['person_id']), axis=1)
        backed_up_awards = list(df_db_awards['db_awards'].unique())
    else:
        backed_up_awards = []

    df_awards_distribution['new_awards'] = df_awards_distribution.apply(lambda row: str(row['award_id']) + '^' +
                                                                                    str(row['event_year']) + '^' +
                                                                                    str(row['content_id']) + '^' +
                                                                                    str(row['person_id']), axis=1)

    df_awards_distribution = df_awards_distribution[~(df_awards_distribution.new_awards.isin(backed_up_awards))]

    df_awards_distribution[['award_id', 'event_year', 'content_id', 'person_id', 'nomination_notes', 'won']].to_csv(
        to_upload_folder + 'awards_distribution.csv', sep='^', index=False)

    conn.close()

    upload_command = 'scp -i ' + config['pem_key'] + ' ' + to_upload_folder + 'awards_distribution.csv ' + config['ec2'][
        'user'] + '@' + config['ec2']['public_dns'] + ':' + config['ec2']['file_upload_location']
    os.system(upload_command)

    return True


def live_search():
    df_content_search = content_search()

    try:
        df_artists_search = pd.read_csv(upload_resources_folder + 'artists_to_contents.csv')
    except:
        print('Calculating artists to contents...')
        artists_to_contents()
        df_artists_search = pd.read_csv(upload_resources_folder + 'artists_to_contents.csv')
    df_artists_search['contents'] = df_artists_search['contents'].apply(lambda array: eval(array) if type(array) == str else None)

    try:
        df_unique_tags = pd.read_csv(upload_resources_folder + 'tags_to_contents.csv')
    except:
        print('Calculating tags to contents...')
        tags_to_contents()
        df_unique_tags = pd.read_csv(upload_resources_folder + 'tags_to_contents.csv')
    df_unique_tags['contents'] = df_unique_tags['contents'].apply(lambda array: eval(array) if type(array) == str else None)

    df_explore = pd.concat([df_content_search, df_artists_search, df_unique_tags], axis=0)

    df_explore = pd.concat([df_explore[pd.notnull(df_explore['contents'])],
                            df_explore[pd.isnull(df_explore['contents'])]], axis=0)
    df_explore.drop_duplicates(['subject', 'subject_id', 'type'], inplace=True)

    df_explore['contents'] = df_explore['contents'].apply(lambda array: str(set(array)).replace("'", '') if type(array) == list else None)

    df_explore = df_explore[~((df_explore['type'] != 'content') & pd.isnull(df_explore['contents']))]
    df_explore.rename(columns={'type': 'subject_type'}, inplace=True)

    int_columns = ['subject_id']
    for col in int_columns:
        df_explore[col][pd.notnull(df_explore[col])] = df_explore[col][pd.notnull(df_explore[col])].apply(
            lambda x: eval(str(x).replace(',', '')))
        df_explore[col][pd.notnull(df_explore[col])] = df_explore[col][pd.notnull(df_explore[col])].apply(
            lambda x: '{:.0f}'.format(x))

    df_explore[['image', 'popularity', 'subject', 'subject_id', 'subject_type', 'contents']].to_csv(to_upload_folder + 'explore_single_word_tags.csv', sep='^', index=False)

    upload_command = 'scp -i ' + config['pem_key'] + ' ' + to_upload_folder + 'explore_single_word_tags.csv ' + \
                     config['ec2'][
                         'user'] + '@' + config['ec2']['public_dns'] + ':' + config['ec2']['file_upload_location']
    os.system(upload_command)

    return True


def genre_counter(genres):
    counter = 0
    for genre in genres:
        counter += genre_count.get(genre, 0)
    return counter


def calculate_similar(content):
    df_resp = df_catg_contents[df_catg_contents['content_id'] == content]

    choice = df_resp['genres'].sum()
    global genre_count
    genre_count = {genre: choice.count(genre) for genre in choice}

    df_resp['genre_counter'] = df_resp['genres'].apply(genre_counter)

    df_resp_mean_attribs, variances = get_features_recom(df_resp, config['algo']['weight_power'])

    df_filtered = filter_data_recom(content, df_catg_contents_all_features, df_clusters)
    if not df_filtered.empty:
        df_filtered['genre_counter'] = df_filtered['genres'].apply(genre_counter)

        df, indices = get_recommendations_recom(df_filtered, df_resp_mean_attribs, config['algo']['neighbours'],
                                                variances)

        int_columns = ['content_id']
        for col in int_columns:
            df_filtered[col][pd.notnull(df_filtered[col])] = df_filtered[col][pd.notnull(df_filtered[col])].apply(
                lambda x: eval(str(x).replace(',', '')))
            df_filtered[col][pd.notnull(df_filtered[col])] = df_filtered[col][pd.notnull(df_filtered[col])].apply(
                lambda x: '{:.0f}'.format(x))

        if df is not False:
            int_columns = ['content_id']
            for col in int_columns:
                df[col][pd.notnull(df[col])] = df[col][pd.notnull(df[col])].apply(
                    lambda x: eval(str(x).replace(',', '')))
                df[col][pd.notnull(df[col])] = df[col][pd.notnull(df[col])].apply(lambda x: '{:.0f}'.format(x))

            content_neighbours = list(df.iloc[indices[0], :]['content_id'].unique())
        else:
            content_neighbours = []

        filter_contents = list(df_filtered['content_id'].unique())
        knn_similar_contents = content_neighbours
    else:
        filter_contents = []
        knn_similar_contents = []

    return filter_contents, knn_similar_contents


def apply_calculate_similar(df):
    if not df.empty:
        df['filter_contents'], df['knn_similar_contents'] = zip(*df['content_id'].apply(calculate_similar))
    return df


def calculate_similar_contents(content_ids=None, df_prev_similar=None, return_dataframe=False):
    thread = Thread(target=keep_alive_connection_for_similar_contents)
    thread.start()

    df_contents_all_features = pd.read_csv('/tmp/full_data.csv', sep='^')
    df_contents_all_features['genres'] = df_contents_all_features['genres'].apply(lambda x: eval(x) if x else None)
    df_contents_all_features['language'] = df_contents_all_features['language'].apply(lambda x: eval(x) if x else None)

    def clean_array(array):
        output = []
        if type(array) == list:
            for item in array:
                output.append(item.replace("'", '').strip())
            return output
        else:
            return None

    global df_clusters
    print('Calculating synonyms similar contents...')
    if content_ids:
        df_prev_clusters = pd.read_csv('/tmp/synonyms_similar_contents.csv')
        df_prev_clusters['synonym_tags'] = df_prev_clusters['synonym_tags'].apply(
            lambda x: clean_array(eval(x)) if str(x).lower() not in ['none', 'nan'] else x)
        df_prev_clusters['genres'] = df_prev_clusters['genres'].apply(
            lambda x: clean_array(eval(x)) if str(x).lower() not in ['none', 'nan'] else x)
        df_prev_clusters['language'] = df_prev_clusters['language'].apply(
            lambda x: clean_array(eval(x)) if str(x).lower() not in ['none', 'nan'] else x)
        df_prev_clusters['common_contents'] = df_prev_clusters['common_contents'].apply(lambda x: eval(x))
        synonyms_similar_contents(content_ids, df_prev_clusters)
    else:
        synonyms_similar_contents()
    df_clusters = pd.read_csv('/tmp/synonyms_similar_contents.csv')
    df_clusters['common_contents'] = df_clusters['common_contents'].apply(lambda x: eval(x))

    engine = sqlalchemy.create_engine('postgres://' + config['sql']['user'] + ':' +
                                      config['sql']['password'] + '@' +
                                      config['sql']['host'] + ':' +
                                      str(config['sql']['port']) + '/' +
                                      config['sql']['db'])

    conn = engine.connect()
    df_contents = pd.read_sql("""
                               select content_id
                               from """ + config['sql']['schema'] + """.content_details
                               """, con=conn)
    conn.close()

    df_contents = pd.merge(df_contents, df_contents_all_features, how='left', on='content_id')
    df_contents = df_contents[pd.notnull(df_contents['genres'])]

    df_similar_contents = pd.DataFrame()
    content_types = ['mv', 'tv']
    for content_type in content_types:
        if content_type == 'mv':
            id_start_digit = 1
        elif content_type == 'tv':
            id_start_digit = 2
        print('\nCalculating similar content for', 'movies...' if content_type == 'mv' else 'tv_series...')
        print('\n')
        global df_catg_contents
        df_catg_contents = df_contents[df_contents['content_id'].astype(str).str.contains('^'+str(id_start_digit))]

        global df_catg_contents_all_features
        df_catg_contents_all_features = df_contents_all_features[df_contents_all_features['content_id'].astype(str).str.contains('^'+str(id_start_digit))]

        if content_ids:
            df_selected_contents = df_catg_contents[df_catg_contents['content_id'].isin(content_ids)]
            df_output = parallelize_dataframe(df_selected_contents, apply_calculate_similar)

            df_output = pd.concat([df_output, df_prev_similar], axis=0)
            df_output.drop_duplicates('content_id', inplace=True)

            if return_dataframe:
                df_output = df_output[['content_id', 'filter_contents', 'knn_similar_contents']]
                return df_output
        else:
            df_output = parallelize_dataframe(df_catg_contents.copy(), apply_calculate_similar)

        df_output = df_output[['content_id', 'filter_contents', 'knn_similar_contents']]
        df_output['content_id'] = df_output['content_id'].apply(lambda x: '{:.0f}'.format(x))
        df_output.rename(columns={'knn_similar_contents': 'similar_contents'}, inplace=True)

        df_similar_contents = pd.concat([df_similar_contents, df_output], axis=0)

    df_similar_contents['similar_contents'] = df_similar_contents['similar_contents'].apply(lambda x: str(x).replace("'", '').replace('[', '{').replace(']', '}').replace('(', '{').replace(')', '}'))
    df_similar_contents['filter_contents'] = df_similar_contents['filter_contents'].apply(lambda x: str(x).replace("'", '').replace('[', '{').replace(']', '}').replace('(', '{').replace(')', '}'))
    df_similar_contents.to_csv('/home/ec2-user/calculated/similar_contents.csv', sep='^', index=False)

    return True


def upload_streaming_info():
    df_streaming = pd.read_csv(streaming_sources_folder + 'cleaned_streaming_sources.csv')
    df_streaming['streaming_sources'] = df_streaming.apply(lambda row: {
        row['streaming_source'].lower().replace(' ', '_'): row['source_url']
    } if str(row['streaming_source']) not in ('nan', 'None') else None, axis=1)

    engine = sqlalchemy.create_engine('postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql']['host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])

    conn = engine.connect()
    df_db_content = pd.read_sql("""
                                   select imdb_content_id, title, release_year
                                   from """ + config['sql']['schema'] + """.content_details
                                   """, con=conn)
    conn.close()
    df_new_movies = pd.read_csv(to_upload_folder + 'movies.csv', sep='^')[['imdb_content_id', 'title', 'release_year']]
    df_new_tv_series = pd.read_csv(to_upload_folder + 'tv_series.csv', sep='^')[['imdb_content_id', 'title', 'release_year']]

    df_all_contents = pd.concat([df_db_content, df_new_movies, df_new_tv_series], axis=0)
    df_all_contents.drop_duplicates(inplace=True)

    df_streaming = pd.merge(df_streaming,
                            df_all_contents,
                            how='left',
                            left_on=['title', 'time_period'],
                            right_on=['title', 'release_year'])
    df_streaming['imdb_id'][pd.isnull(df_streaming['imdb_id'])] = df_streaming['imdb_content_id'][pd.isnull(df_streaming['imdb_id'])]

    df_streaming_grouped = pd.DataFrame(df_streaming[
                                            ['imdb_id', 'stream_type', 'streaming_sources']
                                        ].groupby(['imdb_id', 'stream_type']).streaming_sources.apply(list)).reset_index()

    def list_to_dict(items):
        output_dict = {}
        for item in items:
            output_dict.update(item)
        return output_dict

    df_streaming_grouped['streaming_sources'] = df_streaming_grouped['streaming_sources'].apply(lambda x: list_to_dict(x))

    df_streaming_grouped['streaming_sources'] = df_streaming_grouped.apply(lambda row: {
        row['stream_type'].lower().replace(' ', '_'): row['streaming_sources']
    }, axis=1)

    df_streaming_grouped = pd.DataFrame(df_streaming_grouped.groupby('imdb_id').streaming_sources.apply(list)).reset_index()

    df_streaming_grouped['streaming_sources'] = df_streaming_grouped['streaming_sources'].apply(lambda x: list_to_dict(x))
    df_streaming_grouped.rename(columns= {'streaming_sources': 'where_to_watch'}, inplace=True)

    df_streaming_grouped.to_csv(to_upload_folder + 'streaming_info.csv', sep='^', index=False)

    upload_command = 'scp -i ' + config['pem_key'] + ' ' + to_upload_folder + 'streaming_info.csv ' + config['ec2'][
        'user'] + '@' + config['ec2']['public_dns'] + ':' + config['ec2']['file_upload_location']
    os.system(upload_command)

    return True
