import warnings
warnings.filterwarnings("ignore")

import yaml
import sys
import os
import sqlalchemy
import pandas as pd

from content_data_to_EC2 import *
from utilities import process_spot_instance_data, launch_spot_instance, install_requirements_on_remote, calculate_on_remote, close_spot_fleet_request_and_instances, calculate_crew_table_on_remote


if __name__ == "__main__":
    config = yaml.safe_load(open('./../config.yml'))

    if config['dump_into_database']['calculate_crew_table']:
        if 'operate_spot_instance_to_calculate_crew_table' in sys.argv:
            spot_fleet_request_id, public_dns, private_ip = launch_spot_instance('small')
            print('Public dns -', public_dns)
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
            if config['algo']['calculate_only_for_new_content']:
                print('\nCalculating similar contents for new contents...')

                content_ids = list(pd.read_csv('/tmp/movies.csv', sep='^')['content_id']) + list(
                    pd.read_csv('/tmp/tv_series.csv', sep='^')['content_id'])
                content_ids = list(set(content_ids))
                print('Content ids for 1st stage -', len(content_ids))

                engine = sqlalchemy.create_engine('postgres://' + config['sql']['user'] + ':' +
                                                  config['sql']['password'] + '@' +
                                                  config['sql']['host'] + ':' +
                                                  str(config['sql']['port']) + '/' +
                                                  config['sql']['db'])
                conn = engine.connect()
                df_prev_similar = pd.read_sql("""
                                               select content_id, filtered_content as filter_contents, similar_content as similar_contents
                                               from """ + config['sql']['schema'] + """.content_details
                                               """, con=conn)
                conn.close()

                df_prev_similar = calculate_similar_contents(content_ids, df_prev_similar, True)

                print('\nCalculating similar contents for 2nd stage...')
                content_ids = df_prev_similar['similar_contents'][
                    pd.notnull(df_prev_similar['similar_contents']) & df_prev_similar['content_id'].isin(content_ids)
                ].sum()
                content_ids = list(set(content_ids))
                print('Content ids for 2nd stage -', len(content_ids))
                calculate_similar_contents(content_ids, df_prev_similar, False)
            else:
                calculate_similar_contents()
        elif 'operate_spot_instance_to_calculate' in sys.argv:
            spot_fleet_request_id, public_dns, private_ip = launch_spot_instance()
            print('Public dns -', public_dns)
            install_requirements_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'])

            calculate_on_remote(public_dns, private_ip, 'ec2-user', config['pem_key'], 'calculate_on_spot_instance')

            close_spot_fleet_request_and_instances(spot_fleet_request_id)
        else:
            os.system('start "Calculating similar contents" cmd /k "' + config['venv_path'] + 'python" upload.py operate_spot_instance_to_calculate')
            print('Spot instance launched. Check progress in open terminal.')