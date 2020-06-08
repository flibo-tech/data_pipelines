import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.extend(['./..'])

import pandas as pd
from bs4 import BeautifulSoup
import yaml

from utilities import get_session, should_go_ahead


def movie_budget_n_metacritic_scrape(df_titles):
    titles = list(df_titles['titles'])
    from datetime import datetime # this import was not working somehow when it was above so brought it here

    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['movies_data_folder']

    scrape_start_time = datetime.now()
    i = 1
    j = 0

    session = get_session()
    df_main = pd.DataFrame()

    for title_id in titles:
        url = "http://www.imdb.com/title/" + title_id
        go_ahead, session, html_content = should_go_ahead(url, session, 'title_wrapper')

        if go_ahead:
            try:
                soup = BeautifulSoup(html_content, 'html.parser')

                title_wrapper = soup.find('div', class_='title_wrapper')
                title_name = title_wrapper.text.replace('\xa0', ' ').strip('\n').split('\n')[0].split(' (')[0].strip()
                title_year = title_wrapper.find('a').text.replace('\xa0', ' ').strip().strip('\n')

                subtext = title_wrapper.find('div', class_='subtext')
                try:
                    release_date = subtext.text.replace('\xa0', ' ').strip().strip('\n').split('|')[-1].strip()
                    run_time = subtext.find('time')
                    if run_time:
                        run_time = run_time.text.replace('\xa0', ' ').strip().strip('\n')
                except:
                    release_date = None
                    run_time = None
                try:
                    rating_element = soup.find('div', class_='imdbRating')
                    imdb_rating = rating_element.find('strong').text.replace('\xa0', ' ').strip().strip('\n')
                    num_votes = rating_element.find('a').text.replace('\xa0', ' ').strip().strip('\n').strip().replace(',', '')
                except:
                    imdb_rating = None
                    num_votes = None

                try:
                    summary_text = soup.find('div', class_='summary_text').text.replace('\xa0', ' ').strip().strip('\n').strip()
                except:
                    summary_text = None

                try:
                    metacritic_score = soup.find('div', class_='titleReviewBarItem').text.replace('\xa0', ' ').strip().strip('\n').split('\n')[0]
                except:
                    metacritic_score = None

                try:
                    reviews = soup.find('div', class_='titleReviewBarItem titleReviewbarItemBorder').text.replace('\xa0', ' ').strip().strip('\n').strip()
                except:
                    reviews = None

                try:
                    awards = soup.find('div', class_='article highlighted').text.replace('\xa0', ' ').strip().strip('\n').strip()
                except:
                    awards = None

                try:
                    details = soup.find('div', id='titleDetails').text.replace('\xa0', ' ').strip().strip('\n').strip()
                except:
                    details = None

                try:
                    genres = None
                    elements = soup.findAll('div', class_='see-more inline canwrap')
                    for element in elements:
                        if element.text.replace('\xa0', ' ').strip().strip('\n').count('Genres:') != 0:
                            genres = [x.strip() for x in element.text.replace('\xa0', ' ').strip().strip('\n').replace('Genres:', '').split('|')]
                except:
                    genres = None

                df = pd.DataFrame(
                                  [
                                    {
                                    'title_id':title_id,
                                    'title_name':title_name,
                                    'imdb_rating': imdb_rating,
                                    'num_votes': num_votes,
                                    'release_date': release_date,
                                    'run_time': run_time,
                                    'title_year': title_year,
                                    'genres': genres,
                                    'summary_text':summary_text,
                                    'metacritic_score':metacritic_score,
                                    'reviews':reviews,
                                    'awards':awards,
                                    'details':details
                                    }
                                  ]
                                  )

                if not df.empty:
                    df_main = pd.concat([df_main,df], axis=0)
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
    del df_main

    if not df.empty:
        def country(details):
            try:
                return details.replace('\xa0', ' ').strip().strip('\n').split('Country:')[1].split('\n')[1].split(' | ')
            except:
                return None

        def language(details):
            try:
                return details.replace('\xa0', ' ').strip().strip('\n').split('Language:')[1].split('\n')[1].split(' | ')
            except:
                return None

        def filming_location(details):
            try:
                return \
                details.replace('\xa0', ' ').strip().strip('\n').split('Filming Locations:')[1].split('See more')[0].split('\n')[1]
            except:
                return None

        def production_house(details):
            try:
                return details.replace('\xa0', ' ').strip().strip('\n').split('Production Co:')[1].split('See more')[0].split('\n')[1]
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
                return [x.strip('\n') for x in reviews.split('\n\n')[1].split('|')]
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
