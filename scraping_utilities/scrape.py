import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.extend(['./movies', './tv_series', './streaming_sources', './awards'])

import yaml
import pandas as pd
from multiprocessing import Pool
import numpy as np

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


config = yaml.safe_load(open('./../config.yml'))

if config['scrape_data']['collect_new_imdb_ids']:
    print('--------------------------------- collecting db imdb ids ---------------------------------')
    collect_db_imdb_ids(config)

    print('--------------------------------- collecting new imdb ids ---------------------------------')
    collect_new_imdb_ids()

df_db_ids = pd.read_csv('db_ids.csv').head(10000)
db_ids = list(df_db_ids['imdb_content_id'].unique())

# df_title_ids = pd.read_csv('imdb_ids.csv')
# print('Count of total ids -', df_title_ids.shape[0])
# df_title_ids = df_title_ids[~(df_title_ids['title_id'].isin(db_ids))]
# print('Count of ids after removing already scraped -', df_title_ids.shape[0])


def parallelize_dataframe(titles, func, n_cores=config['algo']['vCPU']):
    df_titles = pd.DataFrame(titles).rename(columns={0:'titles'})
    df_split = np.array_split(df_titles, n_cores)

    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


print('--------------------------------- scraping movies ---------------------------------')
# movies_titles = list(df_title_ids['title_id'][df_title_ids['type']=='feature'].unique())
movies_titles = db_ids
print(len(movies_titles), 'movies to be scraped...')
for scrape_function in config['scrape_data']['movies']:
    print('\n')
    print('----------- scraping data - ' + scrape_function + ' -----------')
    df_temp = parallelize_dataframe(movies_titles, eval(scrape_function))
    df_temp.to_csv('~/final_file.csv', index=False)
    print('\n')
print('--------------------------------- finished scraping movies ---------------------------------\n\n')


# print('--------------------------------- scraping tv_series ---------------------------------')
# tv_series_titles = list(df_title_ids['title_id'][df_title_ids['type']=='tv_series'].unique())
# print(len(tv_series_titles), 'tv_series to be scraped...')
# for scrape_function in config['scrape_data']['tv_series']:
#     print('\n')
#     print('----------- scraping data - ' + scrape_function + ' -----------')
#     eval(scrape_function)(tv_series_titles)
#     print('\n')
# print('--------------------------------- finished scraping tv_series ---------------------------------\n\n')
#
#
# print('--------------------------------- scraping awards ---------------------------------')
# for scrape_function in config['scrape_data']['awards']:
#     eval(scrape_function)()
# print('--------------------------------- finished scraping awards ---------------------------------\n\n')
