import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.extend(['./movies', './tv_series', './streaming_sources', './awards'])

import yaml
import pandas as pd
import os
import time

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
        df_to_scrape = pd.read_csv('titles_to_scrape.csv')
        indices = [int(x) for x in sys.argv[-1].split('-')]
        df_to_scrape = df_to_scrape.iloc[indices[0]:indices[1], :]

        functions = list(df_to_scrape['function'].unique())
        for scrape_function in functions:
            print('Scraping for -', scrape_function)
            titles = list(df_to_scrape['imdb_content_id'][df_to_scrape['function'] == scrape_function].unique())
            df_scraped = parallelize_scraping(titles, eval(scrape_function))
            if scrape_function == 'movie_content_scrape':
                df_movie_certis = df_scraped[df_scraped['df_type'] == 'certis']
                df_movie_certis = df_movie_certis[['title_id', 'certificate_by', 'rating', 'rating_cleaned', 'age_limit', 'pg', 'banned']]
                df_movie_certis.to_csv('/home/ec2-user/scraped/movie_cleaned_certificates_'+sys.argv[-1]+'.csv', index=False)

                df_movie_content = df_scraped[df_scraped['df_type'] == 'content']
                df_movie_content = df_movie_content[['content','content_votes','level','no_of_scenes','text','title_id','title_name']]
                df_movie_content.to_csv('/home/ec2-user/scraped/movie_content_' + sys.argv[-1] + '.csv', index=False)
            else:
                df_scraped.to_csv('/home/ec2-user/scraped/'+scrape_function+'_'+sys.argv[-1]+'.csv', index=False)

    elif config['scrape_data']['prepare_input_for_scrape_using_spot_instance']:
        df_db_ids = pd.read_csv('db_ids.csv')
        df = pd.DataFrame()
        for scrape_function in config['scrape_data']['movies']:
            df_temp = df_db_ids.copy()
            df_temp['function'] = scrape_function
            df = pd.concat([df, df_temp], axis=0)
        df.sort_values('function', inplace=True)
        df.to_csv('titles_to_scrape.csv', index=False)

    elif config['scrape_data']['trigger_scrape_using_spot_instance']:
        if 'scrape_using_spot_instance' in sys.argv:
            spot_fleet_request_id, public_dns, private_ip = launch_spot_instance()
            install_requirements_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'])

            scrape_data_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'], sys.argv[-1])

            cmd = 'scp -r -o StrictHostKeyChecking=no -i '+config['pem_key']+' ec2-user@'+public_dns+':/home/ec2-user/scraped/ '+config['spot_instance_scraped_data_folder']
            os.system('start "Downloading scraped data for index '+sys.argv[-1]+'" /wait cmd /c ' + cmd)

            close_spot_fleet_request_and_instances(spot_fleet_request_id)
        else:
            count = pd.read_csv('titles_to_scrape.csv').head(600).shape[0]
            max_spot_instances = config['scrape_data']['max_spot_instances']
            limit = config['scrape_data']['crawls_per_spot_instance']
            index_ranges = []
            for i in range((count//limit) + (1 if count%limit else 0)):
                index_ranges.append(str(i*limit)+'-'+str(limit*i+limit))

            while index_ranges:
                to_scrape_on = index_ranges[:max_spot_instances]
                for index_range in to_scrape_on:
                    if get_active_spot_fleet_requests_count() < max_spot_instances:
                        print('Triggering scrape for index', index_range)
                        os.system('start "Scraping data for index ' + index_range + '" cmd /k "' + config['venv_path'] + 'python" scrape.py scrape_using_spot_instance ' + index_range)

                        index_ranges.remove(index_range)
                        time.sleep(15)
                    else:
                        break
                print('\nRemaining instances to be triggered -', len(index_ranges))
                if index_ranges:
                    print('Sleeping for 5 minutes, will check then if we can launch more spot instances.\n')
                    time.sleep(5*60)
            print('\n\nRequired number of spot instances launched. Check progress in any open terminals.')

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
