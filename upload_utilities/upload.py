import warnings
warnings.filterwarnings("ignore")

import yaml

from content_data_to_EC2 import *


config = yaml.safe_load(open('./../config.yml'))

for table in config['upload_data']:
    print('\n')
    print('----------- uploading data to EC2 for table ' + table + ' -----------')
    eval(table)()
    print('\n')

if config['algo']['calculate_similar_contents']:
    print('\n')
    print('----------- calculating similar contents -----------')
    similar_contents()