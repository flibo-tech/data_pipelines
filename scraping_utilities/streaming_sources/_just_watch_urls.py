import warnings
warnings.filterwarnings("ignore")

from selenium import webdriver
import pandas as pd
from datetime import datetime
from selenium.common.exceptions import TimeoutException
import time
import yaml

options = webdriver.ChromeOptions()
# options.add_argument('--headless')
# options.add_argument('--window-size=800x800')
options.add_argument("--start-maximized")
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('log-level=3')
# options.add_argument('--proxy-server=45.76.226.206:3128')

config = yaml.safe_load(open('./../config.yml'))
data_folder = config['streaming_sources']

driver = webdriver.Chrome(config['chromedriver'], chrome_options=options)

url_template = 'https://www.justwatch.com/in/provider/streaming_platform/content_type?release_year_from=start_year&release_year_until=end_year'

pause_time = 2.5  # seconds
this_year = 2019
# years = [1900]+[2000+i for i in range(this_year-2000+1)]
years = [2019]
platforms = ['netflix', 'amazon-prime-video', 'hotstar', 'youtube', 'voot', 'hooq', 'viu', 'jio-cinema', 'zee5',
             'eros-now', 'apple-itunes', 'google-play-movies', 'mubi', 'sony-liv', 'guidedoc', 'netflix-kids',
             'tubi-tv', 'yupp-tv']
content = ['movies', 'tv-shows']

for platform in platforms:
    for type in content:
        for year in years:
            if year == 1900:
                start_year = year
                end_year = 2000
            else:
                start_year = year
                end_year = year

            print('Collecting -', platform, '|', type, '|', year)

            url = url_template.replace('streaming_platform', platform)\
                              .replace('content_type', type)\
                              .replace('start_year', str(start_year))\
                              .replace('end_year', str(end_year))
            driver.get(url)

            # Get scroll height
            last_height = driver.execute_script("return document.body.scrollHeight")

            while True:
                # Scroll down to bottom
                # print('Scrolling down bitches!!!')
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

                # Wait to load page
                time.sleep(pause_time)

                # Calculate new scroll height and compare with last scroll height
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    # print("That's enough scrolling for today.")
                    break
                last_height = new_height

            main_content = driver.find_element_by_css_selector('.main-content.row')
            href_tags = main_content.find_elements_by_tag_name('a')
            urls = ['https://www.justwatch.com'+tag.get_attribute('ng-href') for tag in href_tags]

            df = pd.DataFrame({'url_endpoint': urls})
            df.drop_duplicates(inplace=True)
            df['type'] = type
            df['streaming_source'] = platform
            df['streaming_source_region'] = 'India'

            try:
                df_main = pd.read_csv(data_folder+'just_watch_urls.csv')
            except:
                df_main = pd.DataFrame()
            df_main = pd.concat([df_main, df], axis=0)

            df_main.drop_duplicates(inplace=True)
            df_main.to_csv(data_folder+'just_watch_urls.csv', index=False)
            del df_main
            del df

# driver.quit()

