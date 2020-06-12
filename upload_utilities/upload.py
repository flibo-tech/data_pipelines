import warnings
warnings.filterwarnings("ignore")

import yaml
import sys
import os
import sqlalchemy

from content_data_to_EC2 import *
from utilities import process_spot_instance_data, launch_spot_instance, install_requirements_on_remote, calculate_on_remote, close_spot_fleet_request_and_instances, calculate_crew_table_on_remote


if __name__ == "__main__":
    config = yaml.safe_load(open('./../config.yml'))

    if config['dump_into_database']['calculate_crew_table']:
        if 'operate_spot_instance_to_calculate_crew_table' in sys.argv:
            spot_fleet_request_id, public_dns, private_ip = launch_spot_instance('small')
            install_requirements_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'], postgres=True)

            calculate_crew_table_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'])

            close_spot_fleet_request_and_instances(spot_fleet_request_id)
        else:
            os.system('start "Calculating crew table" cmd /k "' + config['venv_path'] + 'python" upload.py operate_spot_instance_to_calculate_crew_table')
            print('Spot instance launched. Check progress in open terminal.')

    if config['upload_data']['use_data_scraped_using_spot_instances']:
        process_spot_instance_data()

    for table in config['upload_data']['scripts']:
        print('\n')
        print('----------- uploading data to EC2 for table ' + table + ' -----------')
        eval(table)()
        print('\n')

    for table in config['dump_into_database']['tables']:
        print('\n')
        print('----------- dumping data into table ' + table + ' -----------')
        engine = sqlalchemy.create_engine(
            'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
                'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])
        eval('dump_'+table)(engine)
        print('\n')

    if config['algo']['calculate_similar_contents']:
        if 'calculate_on_spot_instance' in sys.argv:
            print('\n')
            print('----------- calculating similar contents -----------')
            calculate_similar_contents()
        elif 'operate_spot_instance_to_calculate' in sys.argv:
            spot_fleet_request_id, public_dns, private_ip = launch_spot_instance()
            install_requirements_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'])

            calculate_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'], 'calculate_on_spot_instance')

            cmd = 'scp -r -o StrictHostKeyChecking=no -i ' + config[
                'pem_key'] + ' ec2-user@' + public_dns + ':/home/ec2-user/calculated/ ' + config['to_upload']
            os.system('start "Downloading calculated similar contents" /wait cmd /c ' + cmd)

            close_spot_fleet_request_and_instances(spot_fleet_request_id)
        else:
            os.system('start "Calculating similar contents" cmd /k "' + config['venv_path'] + 'python" upload.py operate_spot_instance_to_calculate')
            print('Spot instance launched. Check progress in open terminal.')