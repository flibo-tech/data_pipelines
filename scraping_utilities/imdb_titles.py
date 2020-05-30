import warnings
warnings.filterwarnings("ignore")

from selenium import webdriver
import pandas as pd
from selenium.common.exceptions import NoSuchElementException
import yaml
from datetime import date
import sqlalchemy


def collect_db_imdb_ids(config):
    engine = sqlalchemy.create_engine(
        'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
            'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])
    conn = engine.connect()

    df_db_ids = pd.read_sql("""
                                select distinct imdb_content_id
                                from """ + config['sql']['schema'] + """.content_details
                            """, con=conn)
    df_db_ids.to_csv('db_ids.csv', index=False)

    return True


def collect_new_imdb_ids():
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--window-size=800x800')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('log-level=3')
    # options.add_argument('--proxy-server=45.76.226.206:3128')

    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['movies_data_folder']



    today = date.today()
    today = str(today.year)+'-'+str(today.month)+'-'+str(today.day)

    for content_type in ['feature', 'tv_series']:
        for language in ['en', 'hi']:
            print('\n\nCollecting ids for -', content_type, '+', language)
            url = 'https://www.imdb.com/search/title/?title_type='+content_type+'&languages='+language+'&count=250&release_date='+config['scrape_data']['latest_release_date']+','+today
            print(url)

            browser = webdriver.Chrome(config['chromedriver'], chrome_options=options)
            browser.get(url)

            title_ids = []
            keep_scraping = True
            while keep_scraping:
                titles = browser.find_elements_by_css_selector('.lister-item.mode-advanced')
                for title in titles:
                    title_block = title.find_element_by_class_name('lister-item-header')
                    element = title_block.find_element_by_tag_name('a')
                    title_id = element.get_attribute('href').split('title/')[1].split('/')[0]
                    title_name = element.text
                    title_year = title_block.find_element_by_css_selector('span.lister-item-year.text-muted.unbold').text,
                    votes = title.find_elements_by_class_name('sort-num_votes-visible')
                    if votes:
                        votes = votes[0].text
                    else:
                        votes = None

                    title_ids.append({
                        'title_id': title_id,
                        'title_name': title_name,
                        'title_year': title_year,
                        'type': content_type,
                        'language': language,
                        'votes': votes
                    })

                print(len(title_ids))

                try:
                    next_page = browser.find_element_by_class_name('desc').find_element_by_css_selector('a.lister-page-next.next-page')
                    next_page.click()
                    print('Scraping next page...')
                except NoSuchElementException:
                    keep_scraping = False

            print('Title ids collected successfully for', content_type, '+', language, '...')

            try:
                df_title_ids = pd.read_csv('imdb_ids.csv')
            except:
                df_title_ids = pd.DataFrame()

            df_title_ids = pd.concat([df_title_ids, pd.DataFrame(title_ids)], axis=0)
            df_title_ids.drop_duplicates(inplace=True)

            df_title_ids.to_csv('imdb_ids.csv', index=False)
            print('Total ids in the csv -', df_title_ids.shape[0])
            del df_title_ids

    print('\nAll title ids collected successfully.')

    return True
