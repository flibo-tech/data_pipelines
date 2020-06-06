import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.extend(['./movies', './tv_series', './streaming_sources', './awards'])

import yaml
import pandas as pd

from movie_budget_n_metacritic_scrape import *
from movie_content_scrape import *
from movie_crew_scrape import *
from movie_keywords_scrape import *
from movie_synopsys_scrape import *
from movie_technical_specs_scrape import *
from movie_tmdb_artist_data_collection import *
from movie_tmdb_data_collection import *

from imdb_titles import *
from tv_series_content_scrape import *
from tv_series_crew_scrape import *
from tv_series_details_scrape import *
from tv_series_keywords_scrape import *
from tv_series_synopsys_scrape import *
from tv_series_technical_specs_scrape import *
from tv_series_tmdb_artist_data_collection import *
from tv_series_tmdb_data_collection import *

from awards_scrape import *

from utilities import *

if __name__ == "__main__":
    config = yaml.safe_load(open('./../config.yml'))

    if 'scrape_on_spot_instance' in sys.argv:
        df_db_ids = pd.read_csv('db_ids.csv').head(8000)
        movies_titles = list(df_db_ids['imdb_content_id'].unique())
        for scrape_function in config['scrape_data']['movies']:
            df_temp = parallelize_scraping(movies_titles, eval(scrape_function))
            df_temp.to_csv('~/final_file.csv', index=False)
    elif config['scrape_data']['trigger_scrape_using_spot_instance']:
        spot_fleet_request_id, public_dns, private_ip = launch_spot_instance()
        install_requirements_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'])
        scrape_data_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'])
        close_spot_fleet_request_and_instances(spot_fleet_request_id)
    else:
        if config['scrape_data']['collect_new_imdb_ids']:
            print('--------------------------------- collecting db imdb ids ---------------------------------')
            collect_db_imdb_ids(config)

            print('--------------------------------- collecting new imdb ids ---------------------------------')
            collect_new_imdb_ids()

        df_db_ids = pd.read_csv('db_ids.csv')
        db_ids = list(df_db_ids['imdb_content_id'].unique())

        df_title_ids = pd.read_csv('imdb_ids.csv')
        print('Count of total ids -', df_title_ids.shape[0])
        df_title_ids = df_title_ids[~(df_title_ids['title_id'].isin(db_ids))]
        print('Count of ids after removing already scraped -', df_title_ids.shape[0])
        print('\n')

        print('--------------------------------- scraping movies ---------------------------------')
        movies_titles = list(df_title_ids['title_id'][df_title_ids['type']=='feature'].unique())
        print(len(movies_titles), 'movies to be scraped...')
        for scrape_function in config['scrape_data']['movies']:
            print('\n')
            print('----------- scraping data - ' + scrape_function + ' -----------')
            eval(scrape_function)(movies_titles)
            print('\n')
        print('--------------------------------- finished scraping movies ---------------------------------\n\n')
        print('\n')

        print('--------------------------------- scraping tv_series ---------------------------------')
        tv_series_titles = list(df_title_ids['title_id'][df_title_ids['type']=='tv_series'].unique())
        print(len(tv_series_titles), 'tv_series to be scraped...')
        for scrape_function in config['scrape_data']['tv_series']:
            print('\n')
            print('----------- scraping data - ' + scrape_function + ' -----------')
            eval(scrape_function)(tv_series_titles)
            print('\n')
        print('--------------------------------- finished scraping tv_series ---------------------------------\n\n')
        print('\n')

        print('--------------------------------- scraping awards ---------------------------------')
        for scrape_function in config['scrape_data']['awards']:
            eval(scrape_function)()
        print('--------------------------------- finished scraping awards ---------------------------------\n\n')
