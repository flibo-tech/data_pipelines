import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from datetime import datetime
import yaml
from bs4 import BeautifulSoup
import re

import sys
sys.path.extend(['./..'])

from utilities import get_session, should_go_ahead


def movie_crew_scrape(df_titles):
    titles = list(df_titles['titles'])

    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['movies_data_folder']

    scrape_start_time = datetime.now()
    i = 1
    j = 0

    session = get_session()
    df_main = pd.DataFrame()

    for title_id in titles:
        url = "http://www.imdb.com/title/" + title_id + "/fullcredits"
        go_ahead, session, html_content = should_go_ahead(url, session, 'subpage_title_block')

        if go_ahead:
            try:
                soup = BeautifulSoup(html_content, 'html.parser')

                title_name = soup.find('div', class_='parent').find('a').text.strip()

                credits = soup.find('div', id='fullcredits_content')
                headers = credits.findAll('h4')
                headers = [re.sub(r'\n{1}\s*', ' ', header.text).strip() for header in headers]

                tables = credits.findAll('table')
                df = pd.DataFrame()
                k = 0
                for table in tables:
                    df_temp = pd.read_html(str(table))[0]
                    df_temp.dropna(how='all', inplace=True)

                    df_temp[0][pd.isnull(df_temp[0])] = 'None'
                    df_temp = df_temp[df_temp[0] != 'Rest of cast listed alphabetically:']
                    df_temp[0][df_temp[0] == 'None'] = pd.np.nan
                    df_temp[0][pd.isnull(df_temp[0])] = df_temp[1][pd.isnull(df_temp[0])]
                    df_temp = df_temp[df_temp[0] != 'Other cast:']

                    df_temp['person_id'] = [x.find('a')['href'].split('/')[2] for x in table.findAll('tr') if
                                            x.find('a') is not None and (
                                                        x.find('a').text.strip() or x.find('a').find('img')[
                                                    'title']) and
                                            x.find('a')['href'] != '#' and x.find('a')['href'].split('/')[2].count(
                                                'nm') == 1]
                    df_temp['credit_category'] = headers[k]
                    df = pd.concat([df, df_temp], axis=0)
                    k += 1

                try:
                    del df[1]
                except:
                    pass
                try:
                    df[3][pd.isnull(df[3])] = df[2][pd.isnull(df[3])]
                except:
                    try:
                        df[3] = df[2]
                    except:
                        df[2] = pd.np.nan
                        df[3] = pd.np.nan
                del df[2]

                try:
                    df = df[pd.notnull(df[0])]
                except:
                    pass
                df = df.reset_index()
                df.rename(columns={'index': 'credit_order', 0: 'person', 3: 'credit_as'}, inplace=True)
                try:
                    df['credit_order'] = df['credit_order'] + 1
                except:
                    pass
                df['title_id'] = title_id
                df['title_name'] = title_name

                df['credit_order_fixed'] = df.groupby(['credit_category'])['credit_order'].rank(ascending=True,
                                                                                                method='first')
                df['credit_order_fixed'] = df['credit_order_fixed'].astype(type(df['credit_order'][0]))
                del df['credit_order']
                df.rename(columns={'credit_order_fixed': 'credit_order'}, inplace=True)

                df_main = pd.concat([df_main, df], axis=0)
                del df
            except Exception as e:
                print('Skipping', title_id, '-', e)
                print('\n')
                j += 1
        else:
            print('Skipping', title_id, '- something wrong.')
            print('\n')
            j += 1
        if i%25 == 0:
            print('Movies scraped -',(i+j))

            time_since_start = (datetime.now()-scrape_start_time).seconds
            all_time_scraping_speed = (i/time_since_start)*3600
            if time_since_start < 60:
                time_since_start = str(time_since_start)+' seconds'
            elif time_since_start < 3600:
                time_since_start = str(time_since_start//60)+ ':'+str(time_since_start%60)+' minutes'
            else:
                time_since_start = str(time_since_start//3600)+ ':'+str((time_since_start%3600)//60)+' hours'
            print('Time since scraping started - '+time_since_start)
            print('All time scraping speed - '+('%.0f'%(all_time_scraping_speed))+' movies/hour')

            try:
                time_since_last_checkpoint = (datetime.now()-time_checkpoint).seconds
            except:
                time_since_last_checkpoint = (datetime.now()-scrape_start_time).seconds
            current_scraping_speed = (25/time_since_last_checkpoint)*3600
            time_remaining = (time_since_last_checkpoint*((len(titles)-i-j)/25))/(3600*24)
            print('Current scraping speed - '+('%.0f'%(current_scraping_speed))+' movies/hour')
            print('Time remaining as per current speed - '+('%.1f'%(time_remaining))+' days')
            print('\n')
            time_checkpoint = datetime.now()
        i += 1



    #################################################################################################################################################################################

                                        #To clean above scraped data

    #################################################################################################################################################################################

    df = df_main.copy()

    if not df.empty:
        df['credit_category'] = df['credit_category'].apply(lambda x: x.replace('  ', ' ').strip())
        df['credit_category'] = df['credit_category'].apply(lambda x: re.sub(r'^Series\s+', '', x))
        df['credit_category'] = df['credit_category'].apply(lambda x: x.replace('  ', ' ').strip())

        credit_rename = {'Cast (in credits order)':'Cast',
                         'Cast (in credits order) complete, awaiting verification':'Cast',
                         'Cast (in credits order) verified as complete':'Cast',
                         'Cast complete, awaiting verification':'Cast',
                         'Cast verified as complete':'Cast',
                         'Series Animation Department':'Animation Department',
                         'Series Camera and Electrical Department':'Camera and Electrical Department',
                         'Series Cast':'Cast',
                         'Series Cinematography by':'Cinematography by',
                         'Series Directed by':'Directed by',
                         'Series Editorial Department':'Editorial Department',
                         'Series Film Editing by':'Film Editing by',
                         'Series Music by':'Music by',
                         'Series Music Department':'Music Department',
                         'Series Produced by':'Produced by',
                         'Series Sound Department':'Sound Department',
                         'Series Thanks':'Thanks',
                         'Writing Credits (in alphabetical order)':'Writing Credits',
                         'Writing Credits (WGA)':'Writing Credits',
                         'Writing Credits (WGA) (in alphabetical order)':'Writing Credits'}

        df['credit_category'] = df['credit_category'].apply(lambda x: credit_rename.get(x, x))

    return df
