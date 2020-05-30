from selenium import webdriver
from pyvirtualdisplay import Display
import pandas as pd
from datetime import datetime
import yaml

# display = Display(visible=0, size=(800, 800))
# display.start()

options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--window-size=800x800')
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--proxy-server=45.76.226.206:3128')

driver = webdriver.Chrome( chrome_options=options)

config = yaml.safe_load(open('./../config.yml'))
data_folder = config['tv_series_data_folder']


def awardee_awards(awardee_id):
    time_start = datetime.now()
    driver.get("http://www.imdb.com/name/"+awardee_id+"/awards?ref_=nm_awd")
    rows_to_add = []
    events = driver.find_elements_by_class_name('awards')
    awardee_name = driver.find_element_by_class_name('subpage_title_block').text.split('\n')[0]
    try:
        no_awards_check = driver.find_element_by_id('no_content')
        rows_to_add.append({'awardee_id':awardee_id,
                            'awardee_name':awardee_name,
                            'awardee_total_nominations':None,
                            'awardee_total_wins':None,
                            'award_year':None,
                            'award_outcome':None,
                            'award_category':None,
                            'award_title':None,
                            'title':None,
                            'title_id':None,
                            'shared_with':[],
                            'award_detail_notes':None})
    except:
        try:
            try:
                awardee_all_time_stat = driver.find_element_by_class_name('desc').text
                awardee_total_nominations = awardee_all_time_stat.split(' wins and ')[1].split(' ')[0]
                awardee_total_wins = awardee_all_time_stat.split(' wins and ')[0].split(' ')[-1]
            except:
                awardee_total_nominations = None
                awardee_total_wins = None

            for event in events:
                awards = event.find_elements_by_tag_name('tr')
                for award in awards:
                    try:
                        award_year = award.find_element_by_class_name('award_year').text
                    except:
                        award_year = award_year # last award_year

                    try:
                        award_outcome = award.find_element_by_class_name('award_outcome').text.split('\n')[0]
                    except:
                        award_outcome = award_outcome # last award_outcome

                    try:
                        award_category = award.find_element_by_class_name('award_category').text
                    except:
                        award_category = award_category # last award_category

                    award_description = award.find_element_by_class_name('award_description')
                    try:
                        award_title = award_description.text.split('\n')[0]
                    except:
                        award_title = None

                    try:
                        title = award_description.find_element_by_css_selector('a[href*="/title/"]')
                        try:
                            title_id = title.get_attribute('href').split('/title/')[1].split('?')[0]
                        except:
                            title_id = None
                        title = title.text
                    except:
                        title = None
                        title_id = None

                    try:
                        title_year = award_description.find_element_by_class_name('title_year').text
                    except:
                        title_year = None

                    try:
                        shared_with_entries = award_description.find_elements_by_css_selector('a[href*="/name/"]')
                        shared_with = []
                        for entry in shared_with_entries:
                            try:
                                entry_id = entry.get_attribute('href').split('/name/')[1].split('?')[0]
                            except:
                                entry_id = None
                            try:
                                entry = entry.text
                            except:
                                entry = None
                            shared_with.append({'entry':entry, 'entry_id':entry_id})
                    except:
                        shared_with = []

                    try:
                        award_detail_notes = award_description.find_element_by_class_name('award_detail_notes').text
                    except:
                        award_detail_notes = None

                    rows_to_add.append({'awardee_id':awardee_id,
                                        'awardee_name':awardee_name,
                                        'awardee_total_nominations':awardee_total_nominations,
                                        'awardee_total_wins':awardee_total_wins,
                                        'award_year':award_year,
                                        'award_outcome':award_outcome,
                                        'award_category':award_category,
                                        'award_title':award_title,
                                        'title':title,
                                        'title_id':title_id,
                                        'shared_with':shared_with,
                                        'award_detail_notes':award_detail_notes})                    
        except:
            rows_to_add.append({'awardee_id':awardee_id,
                                'awardee_name':awardee_name,
                                'awardee_total_nominations':None,
                                'awardee_total_wins':None,
                                'award_year':None,
                                'award_outcome':None,
                                'award_category':None,
                                'award_title':None,
                                'title':None,
                                'title_id':None,
                                'shared_with':[],
                                'award_detail_notes':None})

    try:
        df_awardee_awards = pd.read_csv(data_folder+'awardess_awards.csv')
    except:
        df_awardee_awards = pd.DataFrame()

    df_awardee_awards = df_awardee_awards.append(rows_to_add)
    df_awardee_awards.to_csv(data_folder+'awardess_awards.csv', index=False)
    del df_awardee_awards

    time_end = datetime.now()

    print('   '+awardee_name+' - '+str(len(rows_to_add))+' awards - '+str((time_end-time_start).seconds)+' seconds')

    return None


try:
    df_crew = pd.read_csv(data_folder+'cleaned_tv_series_crew.csv')
    df_crew = pd.concat(
        [df_crew[['person_id']][(df_crew['credit_order'] <= 10) & (df_crew['credit_category'] != 'Cast')],
         df_crew[['person_id']][(df_crew['credit_order'] <= 50) & (df_crew['credit_category'] == 'Cast')]], axis=0)
    awardees = list(df_crew['person_id'].unique())
    del df_crew
except:
    awardees = []

try:
    df_awardee_awards_scraped = pd.read_csv(data_folder + 'awardess_awards.csv')
    awardees_scraped = list(df_awardee_awards_scraped['awardee_id'].unique())
except:
    awardees_scraped = []

scrape_start_time = datetime.now()
i = 0
j = 0
for awardee_id in awardees:
    if awardees_scraped.count(awardee_id) == 0:
        i += 1
        awardee_awards(awardee_id)

        time_since_start = (datetime.now() - scrape_start_time).seconds
        current_scraping_speed = (i / time_since_start) * 3600
        time_remaining = (time_since_start * (len(awardees) - (i + j)) / i) / (3600 * 24)

        if time_since_start < 60:
            time_since_start = str(time_since_start) + ' seconds'
        elif time_since_start < 3600:
            time_since_start = str(time_since_start // 60) + ':' + str(time_since_start % 60) + ' minutes'
        else:
            time_since_start = str(time_since_start // 3600) + ':' + str((time_since_start % 3600) // 60) + ' hours'

        print(str(i) + '.Time since scraping started - ' + time_since_start)
        print('Current scraping speed - ' + ('%.0f' % (current_scraping_speed)) + ' awardees/hour')
        print('Time remaining as per current speed - ' + ('%.1f' % (time_remaining)) + ' days')
        print('\n')
    else:
        j += 1
