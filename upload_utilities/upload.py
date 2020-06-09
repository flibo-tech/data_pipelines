import warnings
warnings.filterwarnings("ignore")

import yaml

from content_data_to_EC2 import *
from utilities import process_spot_instance_data


config = yaml.safe_load(open('./../config.yml'))

if config['upload_data']['use_data_scraped_using_spot_instances']:
    process_spot_instance_data()

for table in config['upload_data']['scripts']:
    print('\n')
    print('----------- uploading data to EC2 for table ' + table + ' -----------')
    eval(table)()
    print('\n')

if config['algo']['calculate_similar_contents']:
    print('\n')
    print('----------- calculating similar contents -----------')
    similar_contents()