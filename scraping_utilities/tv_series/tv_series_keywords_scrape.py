import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from datetime import datetime
import yaml
from bs4 import BeautifulSoup

import sys
sys.path.extend(['./..'])

from utilities import get_session, should_go_ahead


def tv_series_keywords_scrape(df_titles):
    titles = list(df_titles['titles'])

    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['tv_series_data_folder']

    scrape_start_time = datetime.now()
    i = 1
    j = 0

    session = get_session()
    df_main = pd.DataFrame()

    for title_id in titles:
        url = "http://www.imdb.com/title/"+title_id+"/keywords"
        go_ahead, session, html_content = should_go_ahead(url, session, 'subpage_title_block')

        if go_ahead:
            try:
                soup = BeautifulSoup(html_content, 'html.parser')

                title_name = soup.find('div', class_='parent').find('a').text.strip()

                k = 1
                rows_to_add = []
                for element in soup.findAll('td', class_='soda sodavote'):
                    rows_to_add.append({
                                        'title_name':title_name,
                                        'title_id':title_id,
                                        'order':k,
                                        'keyword':element.find('div', class_='sodatext').find('a').text.strip(),
                                        'votes':element.find('div', class_='interesting-count-text').find('a').text.strip(),
                                        })
                    k += 1
                if len(rows_to_add)>0:
                    df_main = df_main.append(rows_to_add)
            except Exception as e:
                print('Skipping', title_id, '-', e)
                print('\n')
                j += 1
        else:
            print('Skipping', title_id, '- something wrong.')
            print('\n')
            j += 1
        if i%25 == 0:
            print('TV Series scraped -',(i+j))

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
        i += 1




    #################################################################################################################################################################################

                                        #To clean above scraped data

    #################################################################################################################################################################################

    import re

    df = df_main.copy()

    def votes(votes):
        upvotes = None
        total_votes = None
        try:
            grps = re.search('^([\d]+)[\D]*([\d]+)[\D]*', votes).groups()
            upvotes = int(grps[0])
            total_votes = int(grps[1])
        except:
            pass
        return upvotes, total_votes

    df['upvotes'], df['total_votes'] = zip(*df['votes'].apply(votes))
    del df['votes']

    return df
