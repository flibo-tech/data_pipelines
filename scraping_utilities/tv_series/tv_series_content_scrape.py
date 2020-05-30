import warnings
warnings.filterwarnings("ignore")

from selenium import webdriver
import pandas as pd
from datetime import datetime
from selenium.common.exceptions import TimeoutException
import time
import yaml


def tv_series_content_scrape(titles):
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
        df_tv_series_content_already_scraped = pd.read_csv(data_folder+'tv_series_content.csv')
        titles_scraped = list(df_tv_series_content_already_scraped['title_id'])
        del df_tv_series_content_already_scraped
    except:
        titles_scraped = []
    print('Scraping history read...')

    certis = []
    tv_series_content = []

    for title_id in titles:
        if titles_scraped.count(title_id) == 0:
            try:
                driver.get("https://www.imdb.com/title/"+title_id+"/parentalguide")
                try:
                    tv_series_name = driver.find_element_by_class_name('subpage_title_block').find_element_by_tag_name('h3').find_element_by_tag_name('a').text
                    go_ahead = True
                except:
                    errors = driver.find_elements_by_class_name('error_message')
                    if errors:
                        if errors[0].text.count('URL was not found') == 0:
                            print('Sleeping for 2 minutes...')
                            time.sleep(2*60)
                            try:
                                driver.get("https://www.imdb.com/title/"+title_id+"/parentalguide")
                                tv_series_name = driver.find_element_by_class_name('subpage_title_block').find_element_by_tag_name('h3').find_element_by_tag_name('a').text
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
                                driver.get("https://www.imdb.com/title/" + title_id + "/parentalguide")
                                tv_series_name = driver.find_element_by_class_name('subpage_title_block').find_element_by_tag_name(
                                    'h3').find_element_by_tag_name('a').text
                                go_ahead = True
                            except:
                                go_ahead = False
                        else:
                            go_ahead = False

                if go_ahead:
                    main_content = driver.find_element_by_css_selector(".article.listo.content-advisories-index")
                    for section in main_content.find_elements_by_tag_name('section'):
                        section_text = section.text.split('\n')
                        section_header = section_text[0]
                        if section_header == 'Certification':
                            try:
                                mpaa_rating = section.find_element_by_id('mpaa-rating').text
                            except:
                                mpaa_rating = None
                            certis.append({'title_id':title_id,
                                           'title_name':tv_series_name,
                                           'certificate_by':'MPAA',
                                           'rating':mpaa_rating})
                            try:
                                certi_list = section.find_element_by_id('certifications-list').find_elements_by_class_name('ipl-inline-list__item')
                            except:
                                certi_list = []
                            if len(certi_list) > 0:
                                for certi in certi_list:
                                    certi = certi.text
                                    certis.append({'title_id':title_id,
                                                   'title_name':tv_series_name,
                                                   'certificate_by':certi.split(':')[0],
                                                   'rating':certi.split(':')[1]})
                        elif section_header == 'Spoilers':
                            break
                        else:
                            no_of_scenes = len(section_text)-2
                            level = section.find_element_by_tag_name('span').text
                            try:
                                content_votes = section.find_element_by_class_name('advisory-severity-vote__message').text
                            except:
                                content_votes = '0 of 0 found this to have none'
                            tv_series_content.append({'title_id':title_id,
                                                  'title_name':tv_series_name,
                                                  'content':section_header,
                                                  'level':level,
                                                  'no_of_scenes':no_of_scenes,
                                                  'content_votes':content_votes,
                                                  'text':section_text})
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
                print('TV Series scraped -', (i + j))

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
                if len(certis) > 0:
                    try:
                        df_tv_series_certis = pd.read_csv(data_folder+'tv_series_certificates.csv')
                    except:
                        df_tv_series_certis = pd.DataFrame()

                    df_tv_series_certis = df_tv_series_certis.append(certis)
                    df_tv_series_certis.to_csv(data_folder+'tv_series_certificates.csv', index=False)
                    del df_tv_series_certis
                    certis = []
                if len(tv_series_content) > 0:
                    try:
                        df_tv_series_content = pd.read_csv(data_folder+'tv_series_content.csv')
                    except:
                        df_tv_series_content = pd.DataFrame()

                    df_tv_series_content = df_tv_series_content.append(tv_series_content)
                    df_tv_series_content.to_csv(data_folder+'tv_series_content.csv', index=False)
                    del df_tv_series_content
                    tv_series_content = []
            i += 1
        else:
            j += 1

    if len(certis) > 0:
        try:
            df_tv_series_certis = pd.read_csv(data_folder+'tv_series_certificates.csv')
        except:
            df_tv_series_certis = pd.DataFrame()

        df_tv_series_certis = df_tv_series_certis.append(certis)
        df_tv_series_certis.to_csv(data_folder+'tv_series_certificates.csv', index=False)
        del df_tv_series_certis
        certis = []
    if len(tv_series_content) > 0:
        try:
            df_tv_series_content = pd.read_csv(data_folder+'tv_series_content.csv')
        except:
            df_tv_series_content = pd.DataFrame()

        df_tv_series_content = df_tv_series_content.append(tv_series_content)
        df_tv_series_content.to_csv(data_folder+'tv_series_content.csv', index=False)
        del df_tv_series_content
        tv_series_content = []



    #################################################################################################################################################################################

                                        #To clean above scraped data (tv_series_certificates.csv)

    #################################################################################################################################################################################

    df = pd.read_csv(data_folder+'tv_series_certificates.csv')

    def rating_cleaned(row):
        rating = None
        try:
            if row['certificate_by'] == 'MPAA':
                if row['rating'].lower().count('rated') != 0:
                    rating = row['rating'].lower().split('rated ')[1].split(' ')[0].upper()
                else:
                    rating = row['rating'].lower().split('mpaa ')[1].split(' ')[0].upper()
            elif ['Finland', 'Portugal', 'Spain'].count(row['certificate_by']) != 1:
                rating = row['rating'].split('/')[0].split(' (')[0].strip()
            else:
                rating = row['rating'].split(' (')[0].strip()
        except:
            pass

        return rating

    df['rating_cleaned'] = df.apply(rating_cleaned, axis=1)


    import re

    def age_limit(rating):
        age_limit = None
        try:
            gp = re.search(r'[\D]*([\d]+).*', rating)
            age_limit = int(gp.groups()[0]) if len(gp.groups()[0])<3 else None
        except:
            pass
        return age_limit
    df['age_limit'] = df['rating_cleaned'].apply(age_limit)



    df_country_certis = pd.read_csv('./../resources/IMDb_countries_certificates.csv', encoding = "ISO-8859-1")

    df = pd.merge(df, df_country_certis, left_on=['rating_cleaned', 'certificate_by'], right_on=['Rating', 'Country'], how='left')


    df['age_limit'][pd.isnull(df['age_limit'])] = df['Age'][pd.isnull(df['age_limit'])]
    df['Banned'][pd.isnull(df['Banned'])] = 0
    df['PG'][pd.isnull(df['PG'])] = 0
    df.rename(columns={
        'PG': 'pg',
        'Banned': 'banned'
    }, inplace=True)
    del df['Age']
    del df['Rating']
    del df['Country']



    df['age_limit'][pd.notnull(df['age_limit'])] = df['age_limit'][pd.notnull(df['age_limit'])].astype(int)
    df['pg'][pd.notnull(df['pg'])] = df['pg'][pd.notnull(df['pg'])].astype(int)
    df['banned'][pd.notnull(df['banned'])] = df['banned'][pd.notnull(df['banned'])].astype(bool)




    df[['title_id', 'certificate_by', 'rating', 'rating_cleaned', 'age_limit', 'pg', 'banned']].to_csv(data_folder+'cleaned_certificates.csv', index=False)

    return True
