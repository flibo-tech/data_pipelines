import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.extend(['./..'])

import pandas as pd
from selenium.common.exceptions import TimeoutException
import yaml
import tempfile
import os
import requests
import time

from utilities import get_driver, get_session


def should_go_ahead(title_id, proxy, driver, session, proxies):
    go_ahead = False
    title_wrapper = None

    try:
        html_content = session.get("http://www.imdb.com/title/" + title_id, timeout=5).text

        tp = tempfile.NamedTemporaryFile(suffix='.html', delete=False)
        tp.write(str.encode('data:text/html;charset=utf-8,' + html_content))
        tp.close()

        # driver.get('file:///' + tp.name)

        # try:
        if html_content.count('title_wrapper') != 0:
            # title_wrapper = driver.find_element_by_class_name('title_wrapper')
            go_ahead = True
            # os.remove(tp.name)
        # except:
        else:
            # errors = driver.find_elements_by_class_name('error_message')
            # if errors:
            #     if errors[0].text.replace('\\n', '').strip().count('URL was not found') != 0:
            if html_content.count('URL was not found') != 0:
                go_ahead = False
                os.remove(tp.name)
            # elif driver.find_element_by_xpath("//*").text.replace('\\n', '').strip().count('Error 503') != 0:
            elif html_content.count('Error 503') != 0:
                os.remove(tp.name)
                if proxies:
                    print('Closing driver for -', title_id, proxy)
                    proxy = proxies.pop()
                    # driver.close()
                    # driver = get_driver(proxy)
                    driver = None
                    session.close()
                    session = get_session(proxy)
                    print('Remaining proxies -', len(proxies))
                    print('\n')
                    try:
                        return should_go_ahead(title_id, proxy, driver, session, proxies)
                    except RecursionError:
                        go_ahead = False
                else:
                    go_ahead = False
                    proxy = None
            else:
                go_ahead = False
                print('No reason found for -', title_id, '-', proxy, '-', tp.name)
                print('\n')
    except requests.exceptions.Timeout:
        if proxies:
            print('Closing driver for -', title_id, proxy)
            proxy = proxies.pop()
            # driver.close()
            # driver = get_driver(proxy)
            driver = None
            session.close()
            session = get_session(proxy)
            print('Remaining proxies -', len(proxies))
            print('\n')
            try:
                return should_go_ahead(title_id, proxy, driver, session, proxies)
            except RecursionError:
                go_ahead = False
        else:
            go_ahead = False
            proxy = None
    except requests.exceptions.ChunkedEncodingError:
        time.sleep(1)
        session.close()
        session = get_session(proxy)
        return should_go_ahead(title_id, proxy, driver, session, proxies)

    return go_ahead, driver, session, proxy, proxies, title_wrapper


def movie_budget_n_metacritic_scrape(df_titles):
    proxies = eval(list(df_titles['proxies'].unique())[0])
    proxy = 'no_proxy'
    titles = list(df_titles['titles'])
    print(len(titles), '-', len(proxies), '-', proxy)
    from datetime import datetime # this import was not working somehow when it was above so brought it here

    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['movies_data_folder']

    scrape_start_time = datetime.now()
    i = 1
    j = 0

    # driver = get_driver(proxy)
    driver = None
    session = get_session(proxy)
    df_main = pd.DataFrame()

    for title_id in titles:
        if proxy:
            time.sleep(1)
            try:
                go_ahead, driver, session, proxy, proxies, title_wrapper = should_go_ahead(title_id, proxy, driver, session, proxies)

                if go_ahead:
                    1
                    # title_name = title_wrapper.text.replace('\\n', '').strip().split('\n')[0].split(' (')[0].strip()
                    # title_year = int(title_wrapper.find_element_by_tag_name('a').text.replace('\\n', '').strip())
                    #
                    # subtext = title_wrapper.find_element_by_class_name('subtext')
                    # try:
                    #     release_date = subtext.text.replace('\\n', '').strip().split('|')[-1].strip()
                    #     run_time = subtext.find_element_by_tag_name('time')
                    #     if run_time:
                    #         run_time = run_time.text.replace('\\n', '').strip()
                    # except:
                    #     release_date = None
                    #     run_time = None
                    # try:
                    #     rating_element = driver.find_element_by_class_name('imdbRating')
                    #     imdb_rating = rating_element.find_element_by_tag_name('strong').text.replace('\\n', '').strip()
                    #     num_votes = rating_element.find_element_by_tag_name('a').text.replace('\\n', '').strip().replace(',', '')
                    # except:
                    #     imdb_rating = None
                    #     num_votes = None
                    #
                    # try:
                    #     summary_text = driver.find_element_by_class_name('summary_text').text.replace('\\n', '').strip()
                    # except:
                    #     summary_text = None
                    #
                    # try:
                    #     metacritic_score = driver.find_element_by_class_name('titleReviewBarItem').text.replace('\\n', '').strip().split('\n')[0]
                    # except:
                    #     metacritic_score = None
                    #
                    # try:
                    #     reviews = driver.find_element_by_css_selector('.titleReviewBarItem.titleReviewbarItemBorder').text.replace('\\n', '').strip()
                    # except:
                    #     reviews = None
                    #
                    # try:
                    #     awards = driver.find_element_by_css_selector('.article.highlighted').text.replace('\\n', '').strip()
                    # except:
                    #     awards = None
                    #
                    # try:
                    #     details = driver.find_element_by_id('titleDetails').text.replace('\\n', '').strip()
                    # except:
                    #     details = None
                    #
                    # try:
                    #     genres = None
                    #     elements = driver.find_elements_by_css_selector('.see-more.inline.canwrap')
                    #     for element in elements:
                    #         if element.text.replace('\\n', '').strip().count('Genres:') != 0:
                    #             genres = [x.strip() for x in element.text.replace('\\n', '').strip().replace('Genres:', '').split('|')]
                    # except:
                    #     genres = None
                    #
                    # df = pd.DataFrame(
                    #                   [
                    #                     {
                    #                     'title_id':title_id,
                    #                     'title_name':title_name,
                    #                     'imdb_rating': imdb_rating,
                    #                     'num_votes': num_votes,
                    #                     'release_date': release_date,
                    #                     'run_time': run_time,
                    #                     'title_year': title_year,
                    #                     'genres': genres,
                    #                     'summary_text':summary_text,
                    #                     'metacritic_score':metacritic_score,
                    #                     'reviews':reviews,
                    #                     'awards':awards,
                    #                     'details':details
                    #                     }
                    #                   ]
                    #                   )
                    #
                    # if not df.empty:
                    #     df_main = pd.concat([df_main,df], axis=0)
                    #     del df
                else:
                    if proxy:
                        print('Skipping', title_id, '- something wrong.')
                    else:
                        print('Skipping', title_id, 'as proxy ended.')
                    print('\n')
                    j += 1
            except TimeoutException as ex:
                titles.append(title_id)
                print('\n')
                print('Skipping '+title_id+' because of Selenium TimeoutException')
                print('\n')
                # driver.close()
                # driver = get_driver(proxy)
                driver = None
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
    del df_main

    if not df.empty:
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

        def budget(details):
            try:
                return details.split('Budget:')[1].split('\r')[0].split(' (')[0].split('\n')[0]
            except:
                return None

        def opening_weekend_USA(details):
            try:
                return details.split('Opening Weekend USA:')[1].split('\r')[0].split(', ')[0].split('\n')[0]
            except:
                return None

        def gross_USA(details):
            try:
                return details.split('Gross USA:')[1].split('\r')[0].split(', ')[0].split('\n')[0]
            except:
                return None

        def gross_worldwide(details):
            try:
                return details.split('Cumulative Worldwide Gross:')[1].split('\r')[0].split(', ')[0].split('\n')[0]
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
        df['budget'] = df['details'].apply(lambda x: budget(x))
        df['opening_weekend_USA'] = df['details'].apply(lambda x: opening_weekend_USA(x))
        df['gross_USA'] = df['details'].apply(lambda x: gross_USA(x))
        df['gross_worldwide'] = df['details'].apply(lambda x: gross_worldwide(x))
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


        def metacritic(score):
            try:
                return int(score)
            except:
                return None
        df['metacritic_score'] = df['metacritic_score'].apply(lambda x: metacritic(x))
        del df['summary_text']




        df.rename(columns={'title_year':'release_year'}, inplace=True)




        from currency_converter import CurrencyConverter
        from datetime import datetime
        c = CurrencyConverter()
        df_curr_xc = pd.read_csv('./../resources/currency_exchange.csv')
        df_inflation = pd.read_csv('./../resources/usd_inflation.csv')
        curr_mapping = {
            '£':'GBP',
            '€':'EUR',
            '$':'USD'
        }
        def normalize_budget(budget, year):
            last_date = None
            currency = None
            try:
                year = int(year)
            except:
                year = None
            if budget and year:
                currency = re.search('^([\D]+)', budget).group(1).strip()
                budget = float(budget.replace(currency,'').replace(',','').replace(' ','').strip())
                currency = curr_mapping.get(currency, currency)
                try:
                    if currency != 'USD':
                        budget = c.convert(budget, currency, new_currency='USD', date=datetime.strptime(str(year)+'/1/1', '%Y/%m/%d'))
                    price_old = df_inflation['value'][df_inflation['year']==year].asobject[0]
                    price_new = df_inflation['value'][df_inflation['year']==2018].asobject[0]
                    budget = budget*(price_new/price_old)
                    currency = 'USD'
                    year = 2018
                except:
                    try:
                        if currency != 'USD':
                            rate = df_curr_xc['rate'][(df_curr_xc['currency']==currency) & (df_curr_xc['year']==year)].asobject[0]
                            budget = rate*budget
                        price_old = df_inflation['value'][df_inflation['year']==year].asobject[0]
                        price_new = df_inflation['value'][df_inflation['year']==2018].asobject[0]
                        budget = budget*(price_new/price_old)
                        currency = 'USD'
                        year = 2018
                    except:
                        pass
            return {
                'currency': currency,
                'amount': budget,
                'year': year
            }



        df['budget'] = df.apply(lambda row: normalize_budget(row['budget'], row['release_year']), axis=1)
        df['opening_weekend_USA'] = df.apply(lambda row: normalize_budget(row['opening_weekend_USA'], row['release_year']), axis=1)
        df['gross_USA'] = df.apply(lambda row: normalize_budget(row['gross_USA'], row['release_year']), axis=1)
        df['gross_worldwide'] = df.apply(lambda row: normalize_budget(row['gross_worldwide'], row['release_year']), axis=1)

    return df
