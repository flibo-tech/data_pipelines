import warnings
warnings.filterwarnings("ignore")

import requests
import time
import pandas as pd
from datetime import datetime
import yaml


def tv_series_tmdb_data_collection(df_titles):
    titles = list(df_titles['titles'])

    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['tv_series_data_folder']
    tmdb_api_key = config['tmdb_api_key']

    scrape_start_time = datetime.now()
    i = 0
    j = 0
    details = []
    for title in titles:
        external_id_url = 'https://api.themoviedb.org/3/find/'+title+'?api_key='+tmdb_api_key+'&language=en-US&external_source=imdb_id'
        response = requests.get(external_id_url)

        if response.status_code == 429:
            print('Received status code 429, sleeping for 10 seconds...')
            time.sleep(5)
            response = requests.get(external_id_url)

        response = response.json().get('tv_results')
        if response:
            tmdb_id = response[0]['id']
            tmdb_details_url = 'https://api.themoviedb.org/3/tv/'+str(tmdb_id)+'?api_key='+tmdb_api_key+'&language=en-US'
            tmdb_details = requests.get(tmdb_details_url).json()

            tmdb_videos_url = 'https://api.themoviedb.org/3/tv/'+str(tmdb_id)+'/videos?api_key='+tmdb_api_key+'&language=en-US'
            video_response = requests.get(tmdb_videos_url).json()
            tmdb_details['tmdb_videos'] = video_response.get('results')

            tmdb_social_ids_url = 'https://api.themoviedb.org/3/tv/'+str(tmdb_id)+'/external_ids?api_key='+tmdb_api_key+'&language=en-US'
            social_ids_response = requests.get(tmdb_social_ids_url).json()
            tmdb_details['social_ids'] = social_ids_response

            title_details = {
                'imdb_id': title,
                'tmdb_id': tmdb_id,
                'tmdb_details': tmdb_details
            }
            details.append(title_details)
            i += 1
        else:
            details.append({'imdb_id': title})
            i += 1

        if i%25 == 0:
            print('tv_series scraped -',(i+j))

            time_since_start = (datetime.now()-scrape_start_time).seconds
            all_time_scraping_speed = (i/time_since_start)*3600
            if time_since_start < 60:
                time_since_start = str(time_since_start)+' seconds'
            elif time_since_start < 3600:
                time_since_start = str(time_since_start//60)+ ':'+str(time_since_start%60)+' minutes'
            else:
                time_since_start = str(time_since_start//3600)+ ':'+str((time_since_start%3600)//60)+' hours'
            print('Time since scraping started - '+time_since_start)
            print('All time scraping speed - '+('%.0f'%(all_time_scraping_speed))+' tv_series/hour')

            try:
                time_since_last_checkpoint = (datetime.now()-time_checkpoint).seconds
            except:
                time_since_last_checkpoint = (datetime.now()-scrape_start_time).seconds
            current_scraping_speed = (25/time_since_last_checkpoint)*3600
            time_remaining = (time_since_last_checkpoint*((len(titles)-i-j)/25))/(3600*24)
            print('Current scraping speed - '+('%.0f'%(current_scraping_speed))+' tv_series/hour')
            print('Time remaining as per current speed - '+('%.1f'%(time_remaining))+' days')
            print('\n')
            time_checkpoint = datetime.now()

    df = pd.DataFrame(details)
    if not df.empty:
        df['tmdb_details'] = df['tmdb_details'].astype(str)
        df = df[['imdb_id', 'tmdb_id', 'tmdb_details']]



        #######################################################################################################
        # Cleaning above data
        #######################################################################################################

        language_mapping = {
            'bn': 'Bengali',
            'de': 'German',
            'en': 'English',
            'es': 'Spanish',
            'fr': 'French',
            'hi': 'Hindi',
            'id': 'Indonesian',
            'it': 'Italian',
            'ja': 'Japanese',
            'kn': 'Kannada',
            'ml': 'Malayalam',
            'mr': 'Marathi',
            'pa': 'Punjabi',
            'pt': 'Portuguese',
            'ru': 'Russian',
            'ta': 'Tamil',
            'te': 'Telugu',
            'tl': 'Tagalog'
        }

        tmdb_image_base_url = 'https://image.tmdb.org/t/p/w500'
        df['cover_photo'] = df['tmdb_details'].apply(lambda x: eval(x).get('backdrop_path') if str(x) != 'nan' else None)
        df['cover_photo'] = df['cover_photo'].apply(lambda x: tmdb_image_base_url+x if x else x)

        df['poster'] = df['tmdb_details'].apply(lambda x: eval(x).get('poster_path') if str(x) != 'nan' else None)
        df['poster'] = df['poster'].apply(lambda x: tmdb_image_base_url+x if x else x)

        df['original_language'] = df['tmdb_details'].apply(lambda x: language_mapping.get(eval(x).get('original_language')) if str(x) != 'nan' else None)

        df['homepage'] = df['tmdb_details'].apply(lambda x: eval(x).get('homepage') if str(x) != 'nan' else None)

        df['number_of_episodes'] = df['tmdb_details'].apply(
            lambda x: eval(x).get('number_of_episodes') if str(x) != 'nan' else None)
        df['number_of_episodes'][pd.notnull(df['number_of_episodes'])] = df['number_of_episodes'][
            pd.notnull(df['number_of_episodes'])].apply(lambda x: int(x))

        df['number_of_seasons'] = df['tmdb_details'].apply(
            lambda x: eval(x).get('number_of_seasons') if str(x) != 'nan' else None)
        df['number_of_seasons'][pd.notnull(df['number_of_seasons'])] = df['number_of_seasons'][
            pd.notnull(df['number_of_seasons'])].apply(lambda x: int(x))

        df['episode_runtime'] = df['tmdb_details'].apply(lambda x: eval(x).get('episode_run_time') if str(x) != 'nan' else None)
        df['episode_runtime'][pd.notnull(df['episode_runtime'])] = df['episode_runtime'][
            pd.notnull(df['episode_runtime'])].apply(lambda x: int(sum(x) / len(x)) if x else None)

        df['facebook'] = df['tmdb_details'].apply(lambda x: eval(x).get('social_ids', {}).get('facebook_id') if str(x) != 'nan' else None)
        df['instagram'] = df['tmdb_details'].apply(lambda x: eval(x).get('social_ids', {}).get('instagram_id') if str(x) != 'nan' else None)
        df['twitter'] = df['tmdb_details'].apply(lambda x: eval(x).get('social_ids', {}).get('twitter_id') if str(x) != 'nan' else None)

        df['production_companies'] = df['tmdb_details'].apply(lambda x: [y.get('id') for y in eval(x).get('production_companies', [{}])] if str(x) != 'nan' else None)

        df['first_air_date'] = df['tmdb_details'].apply(lambda x: eval(x).get('first_air_date') if str(x) != 'nan' else None)
        df['first_air_date'] = df['first_air_date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").date() if x else None)

        df['last_air_date'] = df['tmdb_details'].apply(lambda x: eval(x).get('last_air_date') if str(x) != 'nan' else None)
        df['last_air_date'] = df['last_air_date'].apply(lambda x: datetime.strptime(x, "%Y-%m-%d").date() if x else None)


        def trailer(items):
            size = 0
            youtube_trailer = None

            season_1_trailer_size = 0
            season_1_trailer = None

            if type(items) == list:
                for item in items:
                    if (item.get('size', -1) > size) and (item.get('type') == 'Trailer') and (item.get('site') == 'YouTube'):
                        size = item.get('size', 0)
                        youtube_trailer = item.get('key')

                    if (item.get('size', -1) > season_1_trailer_size) and (
                            item.get('name', '').lower().count('season 1') != 0) and (item.get('type') == 'Trailer') and (
                            item.get('site') == 'YouTube'):
                        season_1_trailer_size = item.get('size', 0)
                        season_1_trailer = item.get('key')

            if season_1_trailer:
                return season_1_trailer
            else:
                return youtube_trailer


        df['youtube_trailer'] = df['tmdb_details'].apply(lambda x: trailer(eval(x).get('tmdb_videos', [])) if str(x) != 'nan' else None)

        df['in_production'] = df['tmdb_details'].apply(lambda x:eval(x).get('in_production') if str(x) != 'nan' else None)

        df['content_type'] = df['tmdb_details'].apply(lambda x:eval(x).get('type') if str(x) != 'nan' else None)

        df = df.where((pd.notnull(df)), None)

        del df['tmdb_details']

    return df
