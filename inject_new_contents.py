import fileinput
import re
import os
import yaml
import datetime
import time

current_path = '\\'.join(os.path.abspath(__file__).split('\\')[:-1])
config = yaml.safe_load(open('./config.yml'))


def update_config(regex_pattern, new_string):
    filename = 'config.yml'
    with fileinput.FileInput(filename, inplace=True, backup='.bak') as file:
        for line in file:
            print(re.sub(regex_pattern, new_string, line, flags=re.I), end='')

    return True


def push_to_git(files, commit_message):
    if type(files) == list:
        os.system('git add ' + ' '.join(files))
    elif type(files) == str:
        os.system('git add ' + files)
    os.system('git commit -m "' + commit_message + '"')
    os.system('git push')
    return True


# resetting config to start
print('\nResetting config to start...')
update_config(r'vCPU:\s*\d+', 'vCPU: 4')
update_config(r'tables:\s*\[.*\]', 'tables: []')
update_config(r'scripts:\s*\[.*\]', 'scripts: []')
update_config(r'prepare_input_for_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'prepare_input_for_scrape_using_spot_instance: False')
update_config(r'collect_new_imdb_ids:\s*(\bFalse\b|\bTrue\b)', 'collect_new_imdb_ids: False')
update_config(r'trigger_data_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'trigger_data_scrape_using_spot_instance: False')
update_config(r'refresh_imdb_meta_info:\s*(\bFalse\b|\bTrue\b)', 'refresh_imdb_meta_info: False')
update_config(r'trigger_streaming_info_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'trigger_streaming_info_scrape_using_spot_instance: False')
update_config(r'use_data_scraped_using_spot_instances:\s*(\bFalse\b|\bTrue\b)', 'use_data_scraped_using_spot_instances: False')
update_config(r'calculate_crew_table:\s*(\bFalse\b|\bTrue\b)', 'calculate_crew_table: False')
update_config(r'calculate_similar_contents:\s*(\bFalse\b|\bTrue\b)', 'calculate_similar_contents: False')


# scraping new title ids
print('\nScraping new title ids...')
update_config(r'prepare_input_for_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'prepare_input_for_scrape_using_spot_instance: True')
update_config(r'collect_new_imdb_ids:\s*(\bFalse\b|\bTrue\b)', 'collect_new_imdb_ids: True')

command = 'start "Scraping title ids..." /wait cmd /c "cd '+current_path+'\\scraping_utilities & "' + config['venv_path'] + 'python" scrape.py"'
os.system(command)


# scraping new contents
print('\nScraping new contents...')
update_config(r'prepare_input_for_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'prepare_input_for_scrape_using_spot_instance: False')
update_config(r'collect_new_imdb_ids:\s*(\bFalse\b|\bTrue\b)', 'collect_new_imdb_ids: False')
update_config(r'trigger_data_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'trigger_data_scrape_using_spot_instance: True')
latest_date = (datetime.datetime.today()-datetime.timedelta(days=2)).date().strftime('%Y-%m-%d')
update_config(r'latest_release_date:\s*\'\d{4}-\d{2}-\d{2}\'', "latest_release_date: '"+latest_date+"'")

push_to_git([
    './config.yml', './scraping_utilities/new_imdb_title_urls.csv', './scraping_utilities/titles_to_scrape.csv'
], 'scraping new data')

command = 'start "Scraping new contents..." /wait cmd /c "cd '+current_path+'\\scraping_utilities & "' + config['venv_path'] + 'python" scrape.py"'
os.system(command)


go_ahead = input('\n\x1B[30;41m' + 'Have all the scraping scripts finished scraping? (yes/no)\x1B[0m\n')
while go_ahead != 'yes':
    print('\nConfirm once the scripts are finished.')
    time.sleep(1)
    go_ahead = input('\n\x1B[30;41m' + 'Have all the scraping scripts finished scraping? (yes/no)\x1B[0m\n')


# uploading primary data to EC2 machine
print('\nUploading primary data to EC2 machine...')
update_config(r'trigger_data_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'trigger_data_scrape_using_spot_instance: False')
update_config(r'use_data_scraped_using_spot_instances:\s*(\bFalse\b|\bTrue\b)', 'use_data_scraped_using_spot_instances: True')
update_config(r'scripts:\s*\[.*\]', "scripts: ['movies_details', 'tv_series_details', 'artists', 'content_crew', 'content_tags', 'content_certificates', 'live_search']")

os.system('del '+config['upload_resources']+'artists_to_contents.csv')

command = 'start "Uploading primary data to EC2 machine..." /wait cmd /c "cd '+current_path+'\\upload_utilities & "' + config['venv_path'] + 'python" upload.py"'
os.system(command)


# dumping primary data into db
print('\nDumping primary data into db...')
update_config(r'use_data_scraped_using_spot_instances:\s*(\bFalse\b|\bTrue\b)', 'use_data_scraped_using_spot_instances: False')
update_config(r'scripts:\s*\[.*\]', 'scripts: []')
update_config(r'tables:\s*\[.*\]', "tables: ['movies', 'tv_series', 'artists', 'content_tags', 'content_certificates']")

print('To proceed, close the open terminal once the script is complete.\nTerminal header - "Dumping primary data into db..."')
command = 'start "Dumping primary data into db..." /wait cmd /k "cd '+current_path+'\\upload_utilities & "' + config['venv_path'] + 'python" upload.py"'
os.system(command)


# calculating crew data
print('\nCalculating crew data...')
update_config(r'tables:\s*\[.*\]', 'tables: []')
update_config(r'calculate_crew_table:\s*(\bFalse\b|\bTrue\b)', 'calculate_crew_table: True')

push_to_git('./config.yml', 'calculating crew data')

command = 'start "Calculating crew data..." cmd /k "cd '+current_path+'\\upload_utilities & "' + config['venv_path'] + 'python" upload.py"'
os.system(command)


go_ahead = input('\n\x1B[30;41m' + 'Has spot instance, launched for crew data calculation, taken pull of GIT? (yes/no)\x1B[0m\n')
while go_ahead != 'yes':
    print('\nConfirm when this script can proceed to next step.')
    time.sleep(1)
    go_ahead = input('\n\x1B[30;41m' + 'Has spot instance, launched for crew data calculation, taken pull of GIT? (yes/no)\x1B[0m\n')


# scraping streaming info
print('\nScraping streaming info...')
update_config(r'calculate_crew_table:\s*(\bFalse\b|\bTrue\b)', 'calculate_crew_table: False')
update_config(r'vCPU:\s*\d+', 'vCPU: 96')
update_config(r'trigger_streaming_info_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'trigger_streaming_info_scrape_using_spot_instance: True')

push_to_git('./config.yml', 'scraping streaming info')

command = 'start "Scraping streaming info..." cmd /k "cd '+current_path+'\\scraping_utilities & "' + config['venv_path'] + 'python" scrape.py"'
os.system(command)


go_ahead = input('\n\x1B[30;41m' + 'Has spot instance, launched for scraping streaming info, taken pull of GIT? (yes/no)\x1B[0m\n')
while go_ahead != 'yes':
    print('\nConfirm when this script can proceed to next step.')
    time.sleep(1)
    go_ahead = input('\n\x1B[30;41m' + 'Has spot instance, launched for scraping streaming info, taken pull of GIT? (yes/no)\x1B[0m\n')


# calculating similar content...
print('\nCalculating similar content...')
update_config(r'trigger_streaming_info_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'trigger_streaming_info_scrape_using_spot_instance: False')
update_config(r'calculate_similar_contents:\s*(\bFalse\b|\bTrue\b)', 'calculate_similar_contents: True')

push_to_git('./config.yml', 'calculating similar contents for new contents')

go_ahead = input('\n\x1B[30;41m' + 'Is crew calculation complete and have you moved file synonyms_similar_contents.csv to /tmp/ folder on EC2? (yes/no)\x1B[0m\n')
while go_ahead != 'yes':
    print('\nConfirm when this script can proceed to next step.')
    time.sleep(1)
    go_ahead = input('\n\x1B[30;41m' + 'Is crew calculation complete and have you moved file synonyms_similar_contents.csv to /tmp/ folder on EC2? (yes/no)\x1B[0m\n')

command = 'start "Calculating similar content..." cmd /k "cd '+current_path+'\\upload_utilities & "' + config['venv_path'] + 'python" upload.py"'
os.system(command)


go_ahead = input('\n\x1B[30;41m' + 'Have you dumped crew data into db by running script on EC2? (yes/no)\x1B[0m\n')
while go_ahead != 'yes':
    print('\nConfirm when this script can proceed to next step.')
    time.sleep(1)
    go_ahead = input('\n\x1B[30;41m' + 'Have you dumped crew data into db by running script on EC2? (yes/no)\x1B[0m\n')


go_ahead = input('\n\x1B[30;41m' + 'Are similar contents calculation & streaming info scrape complete? (yes/no)\x1B[0m\n')
while go_ahead != 'yes':
    print('\nConfirm when this script can proceed to next step.')
    time.sleep(1)
    go_ahead = input('\n\x1B[30;41m' + 'Are similar contents calculation & streaming info scrape complete? (yes/no)\x1B[0m\n')


# dumping final data into db
print('\nDumping final data into db...')
update_config(r'calculate_similar_contents:\s*(\bFalse\b|\bTrue\b)', 'calculate_similar_contents: False')
update_config(r'tables:\s*\[.*\]', "tables: ['similar_contents', 'streaming_info', 'live_search']")

print('To proceed, close the open terminal once the script is complete.\nTerminal header - "Dumping final data into db..."')
command = 'start "Dumping final data into db..." /wait cmd /k "cd '+current_path+'\\upload_utilities & "' + config['venv_path'] + 'python" upload.py"'
os.system(command)


go_ahead = input('\n\x1B[30;41m' + 'Have you moved files full_data.csv & synonyms_similar_contents.csv to ~/ folder on EC2? (yes/no)\x1B[0m\n')
while go_ahead != 'yes':
    print('\nConfirm when this script can proceed to next step.')
    time.sleep(1)
    go_ahead = input('\n\x1B[30;41m' + 'Have you moved files full_data.csv & synonyms_similar_contents.csv to ~/ folder on EC2? (yes/no)\x1B[0m\n')


go_ahead = input('\n\x1B[30;41m' + 'Have you restarted the servers? (yes/no)\x1B[0m\n')
while go_ahead != 'yes':
    print('\nConfirm when this script can proceed to next step.')
    time.sleep(1)
    go_ahead = input('\n\x1B[30;41m' + 'Have you restarted the servers? (yes/no)\x1B[0m\n')


go_ahead = input('\n\x1B[30;41m' + 'Have you removed all the CSVs from /tmp/ folder on EC2? (yes/no)\x1B[0m\n')
while go_ahead != 'yes':
    print('\nConfirm when this script can proceed to next step.')
    time.sleep(1)
    go_ahead = input('\n\x1B[30;41m' + 'Have you removed all the CSVs from /tmp/ folder on EC2? (yes/no)\x1B[0m\n')


# resetting config
print('\nResetting config...')
update_config(r'vCPU:\s*\d+', 'vCPU: 4')
update_config(r'tables:\s*\[.*\]', 'tables: []')
update_config(r'scripts:\s*\[.*\]', 'scripts: []')
update_config(r'prepare_input_for_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'prepare_input_for_scrape_using_spot_instance: False')
update_config(r'collect_new_imdb_ids:\s*(\bFalse\b|\bTrue\b)', 'collect_new_imdb_ids: False')
update_config(r'trigger_data_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'trigger_data_scrape_using_spot_instance: False')
update_config(r'refresh_imdb_meta_info:\s*(\bFalse\b|\bTrue\b)', 'refresh_imdb_meta_info: False')
update_config(r'trigger_streaming_info_scrape_using_spot_instance:\s*(\bFalse\b|\bTrue\b)', 'trigger_streaming_info_scrape_using_spot_instance: False')
update_config(r'use_data_scraped_using_spot_instances:\s*(\bFalse\b|\bTrue\b)', 'use_data_scraped_using_spot_instances: False')
update_config(r'calculate_crew_table:\s*(\bFalse\b|\bTrue\b)', 'calculate_crew_table: False')
update_config(r'calculate_similar_contents:\s*(\bFalse\b|\bTrue\b)', 'calculate_similar_contents: False')

push_to_git('./config.yml', 'reset config, finished injecting new data into db')

print('\nGreat! We\'re all done. See you next time.')
