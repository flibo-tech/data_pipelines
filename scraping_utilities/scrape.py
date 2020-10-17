import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.extend(['./movies', './tv_series', './streaming_sources', './awards'])

import yaml
import pandas as pd
import os
import time
from threading import Thread

from movie_budget_n_metacritic_scrape import *
from movie_content_scrape import *
from movie_crew_scrape import *
from movie_keywords_scrape import *
from movie_synopsys_scrape import *
from movie_technical_specs_scrape import *
from movie_tmdb_artist_data_collection import *
from movie_tmdb_data_collection import *

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

    if 'scrape_data_on_spot_instance' in sys.argv:
        thread = Thread(target=keep_connection_alive_for_scraping)
        thread.start()

        df_to_scrape = pd.read_csv('titles_to_scrape.csv')
        indices = [int(x) for x in sys.argv[-1].split('-')]
        df_to_scrape = df_to_scrape.iloc[indices[0]:indices[1], :]

        functions = list(df_to_scrape['function'].unique())
        for scrape_function in functions:
            print('Scraping for -', scrape_function)
            titles = list(df_to_scrape['imdb_content_id'][df_to_scrape['function'] == scrape_function].unique())
            df_scraped = parallelize_scraping(titles, eval(scrape_function))
            if scrape_function in ['movie_content_scrape', 'tv_series_content_scrape']:
                df_certis = df_scraped[df_scraped['df_type'] == 'certis']
                df_certis = df_certis[['title_id', 'certificate_by', 'rating', 'rating_cleaned', 'age_limit', 'pg', 'banned']]


                df_content = df_scraped[df_scraped['df_type'] == 'content']
                df_content = df_content[['content','content_votes','level','no_of_scenes','text','title_id','title_name']]

                if scrape_function == 'movie_content_scrape':
                    df_certis.to_csv('/home/ec2-user/scraped/movie_cleaned_certificates_' + sys.argv[-1] + '.csv', index=False)
                    df_content.to_csv('/home/ec2-user/scraped/movie_content_' + sys.argv[-1] + '.csv', index=False)
                else:
                    df_certis.to_csv('/home/ec2-user/scraped/tv_series_cleaned_certificates_' + sys.argv[-1] + '.csv', index=False)
                    df_content.to_csv('/home/ec2-user/scraped/tv_series_content_' + sys.argv[-1] + '.csv', index=False)
            else:
                df_scraped.to_csv('/home/ec2-user/scraped/'+scrape_function+'_'+sys.argv[-1]+'.csv', index=False)

    elif 'scrape_title_ids_on_spot_instance' in sys.argv:
        thread = Thread(target=keep_connection_alive_for_scraping)
        thread.start()

        df_to_scrape = pd.read_csv('new_imdb_title_urls.csv')
        indices = [int(x) for x in sys.argv[-1].split('-')]
        df_to_scrape = df_to_scrape.iloc[indices[0]:indices[1], :]

        print('Scraping for index -', indices[0], '-', indices[1])
        df_scraped = parallelize_scraping(df_to_scrape, get_imdb_titles)
        df_scraped.to_csv('/home/ec2-user/scraped/'+'imdb_title_ids'+'_'+sys.argv[-1]+'.csv', index=False)

    elif 'operate_spot_instance_to_scrape_title_ids' in sys.argv:
        spot_fleet_request_id, public_dns, private_ip = launch_spot_instance()
        install_requirements_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'])

        scrape_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'], 'scrape_title_ids_on_spot_instance',
                         sys.argv[-1])

        cmd = 'scp -r -o StrictHostKeyChecking=no -i ' + config[
            'pem_key'] + ' ec2-user@' + public_dns + ':/home/ec2-user/scraped/ ' + config[
                  'spot_instance_scraped_data_folder']
        os.system('start "Downloading scraped data for index ' + sys.argv[-1] + '" /wait cmd /c ' + cmd)

        close_spot_fleet_request_and_instances(spot_fleet_request_id)

    elif 'collect_streaming_urls_using_spot_instance' in sys.argv:
        if sys.argv[-2] == '1':
            df = pd.read_csv('ec2_1.csv')
            df = df.to_dict(orient='records')[0]
            spot_fleet_request_id = df['spot_fleet_request_id']
            public_dns = df['public_dns']
            private_ip = df['private_ip']
        else:
            public_dns = None
            while (public_dns is None) or (public_dns == ''):
                spot_fleet_request_id, public_dns, private_ip = launch_spot_instance('smallest')
                if (public_dns is None) or (public_dns == ''):
                    close_spot_fleet_request_and_instances(spot_fleet_request_id)
            pd.DataFrame([{
                'spot_fleet_request_id': spot_fleet_request_id,
                'public_dns': public_dns,
                'private_ip': private_ip
            }]).to_csv('ec2_'+sys.argv[-2]+'.csv', index=False)
            install_requirements_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'])

        collect_streaming_urls(public_dns, private_ip, 'ec2-user', config['pem_key'], sys.argv[-1])

    elif 'scrape_streaming_urls_using_spot_instance' in sys.argv:
        df = pd.read_csv('ec2_'+sys.argv[-2]+'.csv')
        df = df.to_dict(orient='records')[0]
        spot_fleet_request_id = df['spot_fleet_request_id']
        public_dns = df['public_dns']
        private_ip = df['private_ip']

        scrape_streaming_info_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'], sys.argv[-1])

        if sys.argv[-2] != '1':
            close_spot_fleet_request_and_instances(spot_fleet_request_id)

    elif config['scrape_data']['prepare_input_for_scrape_using_spot_instance']:
        print('Collecting db IDs')
        collect_db_imdb_ids()
        df_db_ids = pd.read_csv('db_ids.csv')
        db_ids = list(df_db_ids['imdb_content_id'])

        if config['scrape_data']['collect_new_imdb_ids']:
            collect_new_imdb_ids()

        df_new_ids = pd.read_csv('new_imdb_titles.csv')
        df_new_ids = df_new_ids[(~df_new_ids['imdb_content_id'].isin(db_ids)) & (pd.notnull(df_new_ids['imdb_score']))]

        df = pd.DataFrame()
        for scrape_function in config['scrape_data']['movies']:
            df_temp = df_new_ids[df_new_ids['type']=='movie']
            df_temp = df_temp[['imdb_content_id']]
            if config['scrape_data']['extra_titles']['movie']:
                df_temp = df_temp.append([{'imdb_content_id': item} for item in config['scrape_data']['extra_titles']['movie']])
                df_temp.drop_duplicates('imdb_content_id', inplace=True)
            df_temp['function'] = scrape_function
            df = pd.concat([df, df_temp], axis=0)

        for scrape_function in config['scrape_data']['tv_series']:
            df_temp = df_new_ids[df_new_ids['type'] == 'tv']
            df_temp = df_temp[['imdb_content_id']]
            if config['scrape_data']['extra_titles']['tv']:
                df_temp = df_temp.append([{'imdb_content_id': item} for item in config['scrape_data']['extra_titles']['tv']])
                df_temp.drop_duplicates('imdb_content_id', inplace=True)
            df_temp['function'] = scrape_function
            df = pd.concat([df, df_temp], axis=0)

        df.sort_values(['function', 'imdb_content_id'], inplace=True)
        df.to_csv('titles_to_scrape.csv', index=False)

    elif config['scrape_data']['trigger_data_scrape_using_spot_instance']:
        if 'scrape_data_using_spot_instance' in sys.argv:
            spot_fleet_request_id, public_dns, private_ip = launch_spot_instance()
            install_requirements_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'])

            scrape_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'], 'scrape_data_on_spot_instance', sys.argv[-1])

            cmd = 'scp -r -o StrictHostKeyChecking=no -i '+config['pem_key']+' ec2-user@'+public_dns+':/home/ec2-user/scraped/ '+config['spot_instance_scraped_data_folder']
            os.system('start "Downloading scraped data for index '+sys.argv[-1]+'" /wait cmd /c ' + cmd)

            close_spot_fleet_request_and_instances(spot_fleet_request_id)
        else:
            count = pd.read_csv('titles_to_scrape.csv').shape[0]
            trigger_scrape_using_spot_instances(count, 'scrape_data_using_spot_instance')

    elif config['scrape_data']['trigger_streaming_info_scrape_using_spot_instance']:
        # getting URL combos to fetch all the content URLs
        spot_fleet_request_id, public_dns, private_ip = launch_spot_instance('smallest')
        pd.DataFrame([{
            'spot_fleet_request_id': spot_fleet_request_id,
            'public_dns': public_dns,
            'private_ip': private_ip
        }]).to_csv('ec2_1.csv', index=False)
        install_requirements_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'])

        combo_count = collect_streaming_info_url_combos(public_dns, private_ip, 'ec2-user', config['pem_key'])

        # launching spot instances
        trigger_scrape_using_spot_instances(combo_count, 'collect_streaming_urls_using_spot_instance', limit_calc=True, cmd_limit=48)

        go_ahead = input(
            '\n\x1B[30;41m' + 'Have all streaming url collectors finished collecting? (yes/no)\x1B[0m\n')
        while go_ahead != 'yes':
            print('\nConfirm when this script can proceed to next step.')
            time.sleep(1)
            go_ahead = input(
                '\n\x1B[30;41m' + 'Have all streaming url collectors finished collecting? (yes/no)\x1B[0m\n')

        urls_count = collate_streaming_urls(public_dns, private_ip, 'ec2-user', config['pem_key'])

        # relaunching the same spot instances
        trigger_scrape_using_spot_instances(urls_count, 'scrape_streaming_urls_using_spot_instance', limit_calc=True, cmd_limit=48)

        go_ahead = input(
            '\n\x1B[30;41m' + 'Have all streaming url scrapers finished scraping? (yes/no)\x1B[0m\n')
        while go_ahead != 'yes':
            print('\nConfirm when this script can proceed to next step.')
            time.sleep(1)
            go_ahead = input(
                '\n\x1B[30;41m' + 'Have all streaming url scrapers finished scraping? (yes/no)\x1B[0m\n')

        collate_streaming_info(public_dns, private_ip, 'ec2-user', config['pem_key'])

        close_spot_fleet_request_and_instances(spot_fleet_request_id)
        os.system('del ec2_*.csv')

    elif config['scrape_data']['refresh_imdb_meta_info']:
        collect_new_imdb_ids()

        if config['scrape_data']['scrape_title_ids_on'] == 'remote':
            check = input('Scrape completed? (y/n)')
            while check != 'y':
                check = input('Scrape completed? (y/n)')

            spot_instance_scraped_data_folder = config['spot_instance_scraped_data_folder'] + '\\scraped\\'
            key = 'imdb_title_ids'

            df = pd.DataFrame()
            for filename in os.listdir(spot_instance_scraped_data_folder):
                if filename.startswith(key) and filename.endswith('.csv'):
                    print(filename)
                    df = pd.concat([df, pd.read_csv(spot_instance_scraped_data_folder + filename)], axis=0)

            df.drop_duplicates('imdb_content_id', inplace=True)

            for col in ['metascore', 'votes']:
                df[col][pd.notnull(df[col])] = df[col][pd.notnull(df[col])].apply(lambda x: eval(str(x).replace(',', '')))
                df[col][pd.notnull(df[col])] = df[col][pd.notnull(df[col])].apply(lambda x: '{:.0f}'.format(x))

            df.to_csv(config['to_upload']+ 'content_meta_info.csv', index=False, sep='^')

        print('\nUploading meta info file to server...')
        cmd = 'scp -i ' + config['pem_key'] + ' ' + config['to_upload'] + 'content_meta_info.csv ec2-user@' + config['ec2']['public_dns'] + ':/tmp/'
        os.system('start "Uploading meta info file to server" /wait cmd /c ' + cmd)

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
