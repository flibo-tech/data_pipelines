import warnings
warnings.filterwarnings("ignore")


import pandas as pd
from datetime import datetime
import yaml
from bs4 import BeautifulSoup

import sys
sys.path.extend(['./..'])

from utilities import get_session, should_go_ahead


def tv_series_synopsys_scrape(df_titles):
    titles = list(df_titles['titles'])

    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['tv_series_data_folder']

    scrape_start_time = datetime.now()
    i = 1
    j = 0

    session = get_session()
    plots = []

    for title_id in titles:
        url = 'https://www.imdb.com/title/'+title_id+'/plotsummary'
        go_ahead, session, html_content = should_go_ahead(url, session, 'subpage_title_block')

        if go_ahead:
            try:
                soup = BeautifulSoup(html_content, 'html.parser')

                summaries = soup.find('ul', id='plot-summaries-content').findAll('li', class_='ipl-zebra-list__item')
                summaries = [item.find('p').text.strip() for item in summaries]

                synopsys = soup.find('ul', id='plot-synopsis-content').find('li', class_='ipl-zebra-list__item').text.strip()

                plots.append({
                    'title_id':title_id,
                    'summaries':summaries,
                    'synopsys':synopsys
                    })
            except Exception as e:
                print('Skipping', title_id, '-', e)
                print('\n')
                j += 1
        else:
            print('Skipping', title_id, '- something wrong.')
            print('\n')
            j += 1
        if i%25 == 0:
            print('TV series scraped -',i)

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
            time_remaining = (time_since_last_checkpoint*((len(titles)-i-j)/25))/(3600)
            print('Current scraping speed - '+('%.0f'%(current_scraping_speed))+' tv_series/hour')
            print('Time remaining as per current speed - '+('%.1f'%(time_remaining))+' hours')
            print('\n')
            time_checkpoint = datetime.now()
        i += 1

    df_tv_series_synopsys = pd.DataFrame()
    if len(plots) > 0:
        df_tv_series_synopsys = df_tv_series_synopsys.append(plots)



    #################################################################################################################################################################################

                                        #To clean above scraped data (tv_series_synopsys.csv)

    #################################################################################################################################################################################

    return df_tv_series_synopsys
