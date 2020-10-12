import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from datetime import datetime
import yaml
import time
from threading import Thread
import os


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
        if files:
            keep_alive = False

    return True


thread = Thread(target=keep_connection_alive_for_scraping)
thread.start()

config = yaml.safe_load(open('./../../config.yml'))
data_folder = config['streaming_sources']

print('concatenating CSVs...')
df_justwatch_contents = pd.DataFrame()
for filename in os.listdir('/tmp/'):
    if filename.startswith('streaming_urls_') and filename.endswith('.csv'):
        print(filename)
        df_justwatch_contents = pd.concat([
            df_justwatch_contents, pd.read_csv('/tmp/'+filename, sep='^')
        ], axis=0)

df_justwatch_contents.drop_duplicates(inplace=True)
df_justwatch_contents.to_csv('/home/ec2-user/scraped/final_streaming_urls.csv', sep='^', index=False)
