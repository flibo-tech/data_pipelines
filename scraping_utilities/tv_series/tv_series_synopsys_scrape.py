import warnings
warnings.filterwarnings("ignore")

from selenium import webdriver
import pandas as pd
from datetime import datetime
from selenium.common.exceptions import TimeoutException
import time
from sqlalchemy import create_engine
import yaml


def tv_series_synopsys_scrape(titles):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--window-size=800x800')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('log-level=3')
    # options.add_argument('--proxy-server=45.76.226.206:3128')

    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['tv_series_data_folder']

    driver = webdriver.Chrome(config['chromedriver'], chrome_options=options)

    titles = [title for title in titles if title not in ['tt9783078', 'tt8416400', 'tt7939762', 'tt1143137']]

    scrape_start_time = datetime.now()
    i = 1
    j = 0

    try:
        df_tv_series_synopsis_already_scraped = pd.read_csv(data_folder+'tv_series_synopsys.csv')
        titles_scraped = list(df_tv_series_synopsis_already_scraped['title_id'])
        del df_tv_series_synopsis_already_scraped
    except:
        titles_scraped = []
    print('Scraping history read...')

    plots = []

    for title_id in titles:
        if titles_scraped.count(title_id) == 0:
            try:
                driver.get('https://www.imdb.com/title/'+title_id+'/plotsummary')
                try:
                    summaries = driver.find_element_by_id('plot-summaries-content').text.split('\n')
                    go_ahead = True
                except:
                    errors = driver.find_elements_by_class_name('error_message')
                    if errors:
                        if errors[0].text.count('URL was not found') == 0:
                            print('Sleeping for 2 minutes...')
                            time.sleep(2*60)
                            try:
                                driver.get('https://www.imdb.com/title/' + title_id + '/plotsummary')
                                summaries = driver.find_element_by_id('plot-summaries-content').text.split('\n')
                                go_ahead = True
                            except:
                                go_ahead = False
                        else:
                            go_ahead = False
                    else:
                        if driver.find_element_by_xpath("//*").text == '':
                            print('Sleeping for 2 minutes...')
                            time.sleep(2*60)
                            try:
                                driver.get('https://www.imdb.com/title/'+title_id+'/plotsummary')
                                summaries = driver.find_element_by_id('plot-summaries-content').text.split('\n')
                                go_ahead = True
                            except:
                                go_ahead = False
                        else:
                            go_ahead = False

                if go_ahead:
                    synopsys = driver.find_element_by_id('plot-synopsis-content').text

                    plots.append({
                        'title_id':title_id,
                        'summaries':summaries,
                        'synopsys':synopsys
                        })
                else:
                    print('Skipping', title_id, 'as code broke for it.')
                    j += 1
            except TimeoutException as ex:
                titles.append(title_id)
                print('\n')
                print('Skipping '+title_id+' because of Selenium TimeoutException')
                print('\n')
                del driver
                driver = webdriver.Chrome(config['chromedriver'], chrome_options=options)
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

            if i%100 == 0:
                if len(plots) > 0:
                    try:
                        df_tv_series_synopsys = pd.read_csv(data_folder+'tv_series_synopsys.csv')
                    except:
                        df_tv_series_synopsys = pd.DataFrame()

                    df_tv_series_synopsys = df_tv_series_synopsys.append(plots)
                    df_tv_series_synopsys.to_csv(data_folder+'tv_series_synopsys.csv', index=False)
                    del df_tv_series_synopsys
                    plots = []
            i += 1
        else:
            j += 1

    if len(plots) > 0:
        try:
            df_tv_series_synopsys = pd.read_csv(data_folder+'tv_series_synopsys.csv')
        except:
            df_tv_series_synopsys = pd.DataFrame()

        df_tv_series_synopsys = df_tv_series_synopsys.append(plots)
        df_tv_series_synopsys.to_csv(data_folder+'tv_series_synopsys.csv', index=False)
        del df_tv_series_synopsys
        plots = []



    #################################################################################################################################################################################

                                        #To clean above scraped data (tv_series_synopsys.csv)

    #################################################################################################################################################################################

    return True
