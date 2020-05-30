import warnings
warnings.filterwarnings("ignore")

from selenium import webdriver
import pandas as pd
from datetime import datetime
from selenium.common.exceptions import TimeoutException
import time
import lxml.html as LH
import yaml


def movie_technical_specs_scrape(titles):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--window-size=800x800')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('log-level=3')
    # options.add_argument('--proxy-server=173.54.193.242:50200')

    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['movies_data_folder']

    driver = webdriver.Chrome(config['chromedriver'], chrome_options=options)

    scrape_start_time = datetime.now()
    i = 1
    j = 0

    try:
        df_movie_technical_specs_already_scraped = pd.read_csv(data_folder+'movie_technical_specs.csv')
        titles_scraped = list(df_movie_technical_specs_already_scraped['title_id'])
        del df_movie_technical_specs_already_scraped
    except:
        titles_scraped = []
    print('Scraping history read...')

    df_main = pd.DataFrame()

    for title_id in titles:
        if titles_scraped.count(title_id) == 0:
            try:
                url = "http://www.imdb.com/title/"+title_id+"/technical"
                driver.get(url)

                try:
                    title_name = driver.find_element_by_class_name('parent').find_element_by_tag_name('a').text
                    go_ahead = True
                except:
                    errors = driver.find_elements_by_class_name('error_message')
                    if errors:
                        if errors[0].text.count('URL was not found') == 0:
                            print('Sleeping for 2 minutes...')
                            time.sleep(2*60)
                            try:
                                driver.get(url)
                                title_name = driver.find_element_by_class_name('parent').find_element_by_tag_name('a').text
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
                                driver.get(url)
                                title_name = driver.find_element_by_class_name('parent').find_element_by_tag_name('a').text
                                go_ahead = True
                            except:
                                go_ahead = False
                        else:
                            go_ahead = False

                if go_ahead:
                    try:
                        table = driver.find_element_by_tag_name('table')
                        df = pd.read_html(table.get_attribute('outerHTML'))[0]
                        df.rename(columns={0:'spec', 1:'detail'}, inplace=True)
                        df['title_id'] = title_id
                        df['title_name'] = title_name
                    except:
                        df = pd.DataFrame()

                    if not df.empty:
                        df_main = pd.concat([df_main,df], axis=0)
                        del df
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

            if i%100 == 0:
                if df_main.shape[0] > 0:
                    try:
                        df_movie_technical_specs = pd.read_csv(data_folder+'movie_technical_specs.csv')
                    except:
                        df_movie_technical_specs = pd.DataFrame()

                    df_movie_technical_specs = pd.concat([df_movie_technical_specs,df_main], axis=0)
                    df_movie_technical_specs.to_csv(data_folder+'movie_technical_specs.csv', index=False)
                    del df_movie_technical_specs
                    df_main = pd.DataFrame()
            i += 1
        else:
            j += 1

    if df_main.shape[0] > 0:
        try:
            df_movie_technical_specs = pd.read_csv(data_folder+'movie_technical_specs.csv')
        except:
            df_movie_technical_specs = pd.DataFrame()

        df_movie_technical_specs = pd.concat([df_movie_technical_specs,df_main], axis=0)
        df_movie_technical_specs.to_csv(data_folder+'movie_technical_specs.csv', index=False)
        del df_movie_technical_specs
        df_main = pd.DataFrame()

    return True
