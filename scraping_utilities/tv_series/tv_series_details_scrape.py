import warnings
warnings.filterwarnings("ignore")

from selenium import webdriver
import pandas as pd
from datetime import datetime
from selenium.common.exceptions import TimeoutException
import time
import lxml.html as LH
import yaml


def tv_series_details_scrape(titles):
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

    titles = [title for title in titles if title != 'tt8416400']

    scrape_start_time = datetime.now()
    i = 1
    j = 0

    try:
        df_tv_series_details_already_scraped = pd.read_csv(data_folder+'tv_series_details.csv')
        titles_scraped = list(df_tv_series_details_already_scraped['title_id'])
        del df_tv_series_details_already_scraped
    except:
        titles_scraped = []
    print('Scraping history read...')

    df_main = pd.DataFrame()

    for title_id in titles:
        if titles_scraped.count(title_id) == 0:
            try:
                url = "http://www.imdb.com/title/"+title_id
                driver.get(url)

                try:
                    title_wrapper = driver.find_element_by_class_name('title_wrapper')
                    title_name = title_wrapper.text.split('\n')[0].split(' (')[0].strip()
                    go_ahead = True

                except:
                    errors = driver.find_elements_by_class_name('error_message')
                    if errors:
                        if errors[0].text.count('URL was not found') == 0:
                            print('Sleeping for 2 minutes...')
                            time.sleep(2*60)
                            try:
                                driver.get(url)
                                title_wrapper = driver.find_element_by_class_name('title_wrapper')
                                title_name = title_wrapper.text.split('\n')[0].split(' (')[0].strip()
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
                                title_wrapper = driver.find_element_by_class_name('title_wrapper')
                                title_name = title_wrapper.text.split('\n')[0].split(' (')[0].strip()
                                go_ahead = True
                            except:
                                go_ahead = False
                        else:
                            go_ahead = False

                if go_ahead:
                    subtext = title_wrapper.find_element_by_class_name('subtext')

                    try:
                        time_period = subtext.text.split('|')[-1].split('(')[-1].strip().rstrip(')').strip()
                    except:
                        time_period = None

                    try:
                        run_time = subtext.find_element_by_tag_name('time')
                        if run_time:
                            run_time = run_time.text
                    except:
                        run_time = None

                    try:
                        rating_element = driver.find_element_by_class_name('imdbRating')
                        imdb_rating = rating_element.find_element_by_tag_name('strong').text
                        num_votes = rating_element.find_element_by_tag_name('a').text.replace(',', '')
                    except:
                        imdb_rating = None
                        num_votes = None

                    try:
                        summary_text = driver.find_element_by_class_name('summary_text').text
                    except:
                        summary_text = None

                    try:
                        reviews = driver.find_element_by_css_selector('.titleReviewBarItem.titleReviewbarItemBorder').text
                    except:
                        reviews = None

                    try:
                        awards = driver.find_element_by_css_selector('.article.highlighted').text
                    except:
                        awards = None

                    try:
                        details = driver.find_element_by_id('titleDetails').text
                    except:
                        details = None

                    try:
                        elements = driver.find_elements_by_css_selector('.see-more.inline.canwrap')
                        for element in elements:
                            if element.text.count('Genres:') != 0:
                                genres = [x.strip() for x in element.text.replace('Genres:', '').split('|')]
                    except:
                        genres = None

                    df = pd.DataFrame(
                                      [
                                        {
                                        'title_id':title_id,
                                        'title_name':title_name,
                                        'genres': genres,
                                        'imdb_rating': imdb_rating,
                                        'num_votes': num_votes,
                                        'time_period': time_period,
                                        'avg_run_time': run_time,
                                        'summary_text':summary_text,
                                        'reviews':reviews,
                                        'awards':awards,
                                        'details':details
                                        }
                                      ]
                                      )

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

            if i%100 == 0:
                if df_main.shape[0] > 0:
                    try:
                        df_tv_series_details = pd.read_csv(data_folder+'tv_series_details.csv')
                    except:
                        df_tv_series_details = pd.DataFrame()

                    df_tv_series_details = pd.concat([df_tv_series_details,df_main], axis=0)
                    df_tv_series_details.to_csv(data_folder+'tv_series_details.csv', index=False)
                    del df_tv_series_details
                    df_main = pd.DataFrame()
            i += 1
        else:
            j += 1

    if df_main.shape[0] > 0:
        try:
            df_tv_series_details = pd.read_csv(data_folder+'tv_series_details.csv')
        except:
            df_tv_series_details = pd.DataFrame()

        df_tv_series_details = pd.concat([df_tv_series_details,df_main], axis=0)
        df_tv_series_details.to_csv(data_folder+'tv_series_details.csv', index=False)
        del df_tv_series_details
        df_main = pd.DataFrame()



    #################################################################################################################################################################################

                                        #To clean above scraped data

    #################################################################################################################################################################################

    df = pd.read_csv(data_folder+'tv_series_details.csv')

    def country(details):
        try:
            return details.split('Country: ')[1].split('\r')[0].split('\n')[0].split(' | ')
        except:
            return None

    def language(details):
        try:
            return details.split('Language: ')[1].split('\r')[0].split('\n')[0].split(' | ')
        except:
            return None

    def filming_location(details):
        try:
            return details.split('Filming Locations: ')[1].split(' See more')[0].split('\r')[0].split('\n')[0]
        except:
            return None

    def production_house(details):
        try:
            return details.split('Production Co: ')[1].split(' See more')[0].split(',')
        except:
            return None

    def reviews_list(reviews):
        try:
            return reviews.split('\n')[1].split(' | ')
        except:
            return None

    def wins_n_nomintations(awards):
        try:
            return awards.split('. See more')[0]
        except:
            return None

    df['country'] = df['details'].apply(lambda x: country(x))
    df['language'] = df['details'].apply(lambda x: language(x))
    df['filming_location'] = df['details'].apply(lambda x: filming_location(x))
    df['production_house'] = df['details'].apply(lambda x: production_house(x))
    del df['details']
    df['reviews_list'] = df['reviews'].apply(lambda x: reviews_list(x))
    del df['reviews']
    df['wins_n_nomintations'] = df['awards'].apply(lambda x: wins_n_nomintations(x))
    del df['awards']

    def no_of_reviews(reviews_list):
        critic = None
        user = None
        try:
            for item in reviews_list:
                if item.split(' ')[1]=='critic':
                    critic = item.split(' ')[0]
                else:
                    user = item.split(' ')[0]
        except:
            pass
        return critic, user

    df['critic_review'], df['user_review'] = zip(*df['reviews_list'].apply(no_of_reviews))
    del df['reviews_list']

    import re
    def total_win_nominations(wins_n_nomintations):
        gp = re.search('^[Nominated for\s]+([\d]+)', str(wins_n_nomintations), re.I)
        try:
            add_nominations = int(gp.group(1))
        except:
            add_nominations = 0

        gp = re.search('^[Won\s]+([\d]+)', str(wins_n_nomintations), re.I)
        try:
            add_wins = int(gp.group(1))
        except:
            add_wins = 0

        gp = re.search('([\d]+)(?= win)', str(wins_n_nomintations), re.I)
        try:
            total_wins = int(gp.group(1))
        except:
            total_wins = 0

        gp = re.search('([\d]+)(?= nomination)', str(wins_n_nomintations), re.I)
        try:
            total_nominations = int(gp.group(1))
        except:
            total_nominations = 0

        total_wins = total_wins+add_wins
        total_wins = total_wins if total_wins else None
        total_nominations = total_nominations+add_nominations
        total_nominations = total_nominations if total_nominations else None

        return total_wins, total_nominations

    df['award_wins'], df['award_nominations'] = zip(*df['wins_n_nomintations'].apply(total_win_nominations))
    del df['wins_n_nomintations']

    del df['summary_text']

    df.to_csv(data_folder+'cleaned_tv_series_details.csv', index=False)

    return True
