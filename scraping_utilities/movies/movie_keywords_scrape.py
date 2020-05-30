import warnings
warnings.filterwarnings("ignore")

from selenium import webdriver
import pandas as pd
from datetime import datetime
from selenium.common.exceptions import TimeoutException
import time
import lxml.html as LH
import yaml


def movie_keywords_scrape(titles):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--window-size=800x800')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('log-level=3')
    # options.add_argument('--proxy-server=45.76.226.206:3128')

    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['movies_data_folder']

    driver = webdriver.Chrome(config['chromedriver'], chrome_options=options)

    scrape_start_time = datetime.now()
    i = 1
    j = 0

    try:
        df_movie_keywords_already_scraped = pd.read_csv(data_folder+'movie_keywords.csv')
        titles_scraped = list(df_movie_keywords_already_scraped['title_id'])
        del df_movie_keywords_already_scraped
    except:
        titles_scraped = []
    print('Scraping history read...')

    df_main = pd.DataFrame()

    for title_id in titles:
        if titles_scraped.count(title_id) == 0:
            try:
                url = "http://www.imdb.com/title/"+title_id+"/keywords"
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
                    k = 1
                    rows_to_add = []
                    for element in driver.find_elements_by_css_selector('.soda.sodavote'):
                        text = element.text.split('\n')
                        rows_to_add.append({
                                            'title_name':title_name,
                                            'title_id':title_id,
                                            'order':k,
                                            'keyword':text[0],
                                            'votes':text[1],
                                            })
                        k += 1
                    if len(rows_to_add)>0:
                        df_main = df_main.append(rows_to_add)
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
                        df_movie_keywords = pd.read_csv(data_folder+'movie_keywords.csv')
                    except:
                        df_movie_keywords = pd.DataFrame()

                    df_movie_keywords = pd.concat([df_movie_keywords,df_main], axis=0)
                    df_movie_keywords.to_csv(data_folder+'movie_keywords.csv', index=False)
                    del df_movie_keywords
                    df_main = pd.DataFrame()
            i += 1
        else:
            j += 1

    if df_main.shape[0] > 0:
        try:
            df_movie_keywords = pd.read_csv(data_folder+'movie_keywords.csv')
        except:
            df_movie_keywords = pd.DataFrame()

        df_movie_keywords = pd.concat([df_movie_keywords,df_main], axis=0)
        df_movie_keywords.to_csv(data_folder+'movie_keywords.csv', index=False)
        del df_movie_keywords
        df_main = pd.DataFrame()




    #################################################################################################################################################################################

                                        #To clean above scraped data

    #################################################################################################################################################################################

    import re

    df = pd.read_csv(data_folder+'movie_keywords.csv')

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

    df.to_csv(data_folder+'cleaned_movie_keywords.csv', index=False)

    return True
