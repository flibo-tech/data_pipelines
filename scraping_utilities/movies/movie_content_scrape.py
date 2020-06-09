import warnings
warnings.filterwarnings("ignore")

import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import yaml

import sys
sys.path.extend(['./..'])

from utilities import get_session, should_go_ahead


def movie_content_scrape(df_titles):
    import re
    titles = list(df_titles['titles'])

    config = yaml.safe_load(open('./../config.yml'))
    data_folder = config['movies_data_folder']

    scrape_start_time = datetime.now()
    i = 1
    j = 0

    session = get_session()
    certis = []
    movie_content = []

    for title_id in titles:
        url = "https://www.imdb.com/title/" + title_id + "/parentalguide"
        go_ahead, session, html_content = should_go_ahead(url, session, 'subpage_title_block')

        if go_ahead:
            try:
                soup = BeautifulSoup(html_content, 'html.parser')

                movie_name = soup.find('div', class_='subpage_title_block').find('h3').find('a').text

                main_content = soup.find('section', class_='article listo content-advisories-index')
                for section in main_content.findAll('section'):
                    section_header = (section.find('h3') or section.find('h4')).text.strip()
                    if section_header == 'Certification':
                        try:
                            mpaa_rating = section.find('tr', id='mpaa-rating').text
                            mpaa_rating = re.sub(r'\n+', ' ', mpaa_rating).strip()
                        except:
                            mpaa_rating = None
                        certis.append({'title_id': title_id,
                                       'title_name': movie_name,
                                       'certificate_by': 'MPAA',
                                       'rating': mpaa_rating})

                        try:
                            certi_list = section.find('tr', id='certifications-list').findAll('li', class_='ipl-inline-list__item')
                        except:
                            certi_list = []
                        if len(certi_list) > 0:
                            for certi in certi_list:
                                certi = re.sub(r'\n{1}\s+', ' ', certi.text).strip()
                                certis.append({'title_id': title_id,
                                               'title_name': movie_name,
                                               'certificate_by': certi.split(':')[0],
                                               'rating': certi.split(':')[1]})
                    elif section_header == 'Spoilers':
                        break
                    else:
                        scenes = section.findAll('li', class_='ipl-zebra-list__item')
                        scene_texts = [scene.text.replace(' \n\n\n\n\n\nEdit', '').strip() for scene in scenes]
                        no_of_scenes = len(scenes)
                        level = section.find('span').text
                        try:
                            content_votes = section.find('a', class_='advisory-severity-vote__message').text
                        except:
                            content_votes = '0 of 0 found this to have none'
                        movie_content.append({'title_id': title_id,
                                              'title_name': movie_name,
                                              'content': section_header,
                                              'level': level,
                                              'no_of_scenes': no_of_scenes,
                                              'content_votes': content_votes,
                                              'text': scene_texts})
            except Exception as e:
                print('Skipping', title_id, '-', e)
                print('\n')
                j += 1
        else:
            print('Skipping', title_id, '- something wrong.')
            print('\n')
            j += 1
        if i%25 == 0:
            print('Movies scraped -', i)

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

    df_movie_certis = pd.DataFrame()
    if len(certis) > 0:
        df_movie_certis = df_movie_certis.append(certis)

    df_movie_content = pd.DataFrame()
    if len(movie_content) > 0:
        df_movie_content = df_movie_content.append(movie_content)



    #################################################################################################################################################################################

                                        #To clean above scraped data (movie_certificates.csv)

    #################################################################################################################################################################################

    df = df_movie_certis.copy()

    if not df.empty:
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

        df_movie_certis = df[['title_id', 'certificate_by', 'rating', 'rating_cleaned', 'age_limit', 'pg', 'banned']]

    df_movie_certis['df_type'] = 'certis'
    df_movie_content['df_type'] = 'content'

    df = pd.concat([df_movie_certis, df_movie_content], axis=0)

    return df
