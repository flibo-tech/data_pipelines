import warnings
warnings.filterwarnings("ignore")

from selenium import webdriver
import pandas as pd
from datetime import datetime
from selenium.common.exceptions import TimeoutException
import time
import lxml.html as LH
import yaml


def movie_crew_scrape(titles):
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
        df_movie_crew_already_scraped = pd.read_csv(data_folder+'movie_crew.csv')
        titles_scraped = list(df_movie_crew_already_scraped['title_id'])
        del df_movie_crew_already_scraped
    except:
        titles_scraped = []
    print('Scraping history read...')

    df_main = pd.DataFrame()

    for title_id in titles:
        if titles_scraped.count(title_id) == 0:
            try:
                url = "http://www.imdb.com/title/"+title_id+"/fullcredits"
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
                    credits = driver.find_element_by_id('fullcredits_content')
                    headers = credits.find_elements_by_tag_name('h4')
                    headers = [header.text for header in headers]

                    tables = credits.find_elements_by_tag_name('table')
                    df = pd.DataFrame()
                    k = 0
                    for table in tables:
                        df_temp = pd.read_html(table.get_attribute('outerHTML'))[0]
                        df_temp.dropna(how='all', inplace=True)

                        df_temp[0][pd.isnull(df_temp[0])] = 'None'
                        df_temp = df_temp[df_temp[0]!='Rest of cast listed alphabetically:']
                        df_temp[0][df_temp[0]=='None'] = pd.np.nan
                        df_temp[0][pd.isnull(df_temp[0])] = df_temp[1][pd.isnull(df_temp[0])]
                        df_temp = df_temp[df_temp[0] != 'Other cast:']

                        href_temp = LH.fromstring(table.get_attribute('outerHTML'))
                        # person_ids = [x.split('/')[2] for x in href_temp.xpath('//tr/td/a/@href') if x.split('/')[2].count('nm')==1]
                        # person_ids = sorted(set(person_ids), key=person_ids.index)
                        #
                        # people = list(df_temp[0].unique())
                        #
                        # id_dict = {}
                        # for person, person_id in zip(people, person_ids):
                        #     id_dict[person] = person_id
                        # df_temp['person_id'] = df_temp[0].apply(lambda person: id_dict[person])
                        # print(table.text)
                        # print(title_id)
                        df_temp['person_id'] = [x.get('href').split('/')[2] for x in href_temp.xpath('//tr/td/a') if
                                                x.get('href') != '#' and x.get('href').split('/')[2].count(
                                                    'nm') == 1 and x.text]

                        df_temp['credit_category'] = headers[k]
                        df = pd.concat([df,df_temp], axis=0)
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
                    df.rename(columns={'index':'credit_order',0:'person',3:'credit_as'}, inplace=True)
                    try:
                        df['credit_order'] = df['credit_order']+1
                    except:
                        pass
                    df['title_id'] = title_id
                    df['title_name'] = title_name

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
                        df_movie_crew = pd.read_csv(data_folder+'movie_crew.csv')
                    except:
                        df_movie_crew = pd.DataFrame()

                    df_movie_crew = pd.concat([df_movie_crew,df_main], axis=0)
                    df_movie_crew.to_csv(data_folder+'movie_crew.csv', index=False)
                    del df_movie_crew
                    df_main = pd.DataFrame()
            i += 1
        else:
            j += 1

    if df_main.shape[0] > 0:
        try:
            df_movie_crew = pd.read_csv(data_folder+'movie_crew.csv')
        except:
            df_movie_crew = pd.DataFrame()

        df_movie_crew = pd.concat([df_movie_crew,df_main], axis=0)
        df_movie_crew.to_csv(data_folder+'movie_crew.csv', index=False)
        del df_movie_crew
        df_main = pd.DataFrame()



    #################################################################################################################################################################################

                                        #To clean above scraped data

    #################################################################################################################################################################################

    df = pd.read_csv(data_folder+'movie_crew.csv')

    df['credit_category'] = df['credit_category'].apply(lambda x: x.strip())

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
    df.to_csv(data_folder+'movie_crew.csv', index=False)

    return True
