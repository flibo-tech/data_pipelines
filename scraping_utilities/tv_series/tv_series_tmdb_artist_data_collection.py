import requests
import time
import pandas as pd
from datetime import datetime
import yaml


def tv_series_tmdb_artist_data_collection(titles=None):
    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['tv_series_data_folder']
    tmdb_api_key = config['tmdb_api_key']

    try:
        df_crew = pd.read_csv(data_folder+'cleaned_tv_series_crew.csv')
        df_crew = pd.concat(
            [df_crew[['person_id']][(df_crew['credit_order'] <= 10) & (df_crew['credit_category'] != 'Cast')],
             df_crew[['person_id']][(df_crew['credit_order'] <= 10) & (df_crew['credit_category'] == 'Cast')]], axis=0)
        artists = list(df_crew['person_id'].unique())
        del df_crew
    except:
        artists = []

    try:
        df_tv_series_tmdb_artists_already_scraped = pd.read_csv(data_folder + 'tv_series_tmdb_artists.csv')
        artists_scraped = list(df_tv_series_tmdb_artists_already_scraped['imdb_id'].unique())
        del df_tv_series_tmdb_artists_already_scraped
    except:
        artists_scraped = []
    print('Scraping history read...')

    scrape_start_time = datetime.now()
    i = 0
    j = 0
    details = []
    for artist in artists:
        if artists_scraped.count(artist) == 0:
            external_id_url = 'https://api.themoviedb.org/3/find/'+artist+'?api_key='+tmdb_api_key+'&language=en-US&external_source=imdb_id'
            response = requests.get(external_id_url)

            if response.status_code == 429:
                print('Received status code 429, sleeping for 10 seconds...')
                time.sleep(10)
                response = requests.get(external_id_url)

            response = response.json()['person_results']
            if response:
                tmdb_id = response[0]['id']
                tmdb_details_url = 'https://api.themoviedb.org/3/person/'+str(tmdb_id)+'?api_key='+tmdb_api_key+'&language=en-US'
                tmdb_details = requests.get(tmdb_details_url).json()
                details.append(tmdb_details)
                i += 1
            else:
                details.append({'imdb_id': artist})
                i += 1

            if i%25 == 0:
                print('artists scraped -',(i+j))

                time_since_start = (datetime.now()-scrape_start_time).seconds
                all_time_scraping_speed = (i/time_since_start)*3600
                if time_since_start < 60:
                    time_since_start = str(time_since_start)+' seconds'
                elif time_since_start < 3600:
                    time_since_start = str(time_since_start//60)+ ':'+str(time_since_start%60)+' minutes'
                else:
                    time_since_start = str(time_since_start//3600)+ ':'+str((time_since_start%3600)//60)+' hours'
                print('Time since scraping started - '+time_since_start)
                print('All time scraping speed - '+('%.0f'%(all_time_scraping_speed))+' artists/hour')

                try:
                    time_since_last_checkpoint = (datetime.now()-time_checkpoint).seconds
                except:
                    time_since_last_checkpoint = (datetime.now()-scrape_start_time).seconds
                current_scraping_speed = (25/time_since_last_checkpoint)*3600
                time_remaining = (time_since_last_checkpoint*((len(artists)-i-j)/25))/(3600*24)
                print('Current scraping speed - '+('%.0f'%(current_scraping_speed))+' artists/hour')
                print('Time remaining as per current speed - '+('%.1f'%(time_remaining))+' days')
                print('\n')
                time_checkpoint = datetime.now()

            if i%25 == 0:
                df = pd.DataFrame(details)
                try:
                    df_main = pd.read_csv(data_folder + 'tv_series_tmdb_artists.csv')
                except:
                    df_main = pd.DataFrame()
                df_main = pd.concat([df_main, df], axis=0)
                df_main['also_known_as'] = df_main['also_known_as'].astype(str)

                df_main.drop_duplicates(inplace=True)
                df_main.to_csv(data_folder + 'tv_series_tmdb_artists.csv', index=False)
                del df_main
                del df
                details = []
        else:
            j += 1

    df = pd.DataFrame(details)
    try:
        df_main = pd.read_csv(data_folder + 'tv_series_tmdb_artists.csv')
    except:
        df_main = pd.DataFrame()
    df_main = pd.concat([df_main, df], axis=0)
    df_main['also_known_as'] = df_main['also_known_as'].astype(str)

    df_main.drop_duplicates(inplace=True)
    df_main.to_csv(data_folder + 'tv_series_tmdb_artists.csv', index=False)
    del df_main
    del df

    return True
