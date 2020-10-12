import requests
import pandas as pd
import numpy as np
import yaml
from multiprocessing import Pool
from selenium.webdriver.remote.remote_connection import LOGGER, logging
from selenium import webdriver
import boto3
import time
import paramiko
from paramiko_expect import SSHClientInteraction
from requests.exceptions import Timeout, ChunkedEncodingError, ConnectionError
import sqlalchemy
from datetime import datetime, date
from bs4 import BeautifulSoup
import os


LOGGER.setLevel(logging.WARNING)
config = yaml.safe_load(open('./../config.yml'))


def get_driver(proxy=None):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--disable-dev-shm-usage')
    if proxy:
        options.add_argument('--proxy-server=' + proxy)

    driver = webdriver.Chrome(chrome_options=options)

    return driver


def get_session(proxy=None):
    session = requests.Session()
    session.headers.update({
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'none',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36'
    })
    if proxy:
        session.proxies.update({
                'http': 'http://' + proxy,
                'https': 'https://' + proxy,
                'ftp': 'ftp://' + proxy
            })

    return session


def should_go_ahead(url, session, string_to_check, timeout=5):
    go_ahead = False
    html_content = None

    try:
        html_content = session.get(url, timeout=timeout).text

        if html_content.count(string_to_check) != 0:
            go_ahead = True
        else:
            if html_content.count('URL was not found') != 0:
                go_ahead = False
            elif html_content.count('Error 503') != 0:
                print('Error 503, Sleeping for 5 sec...')
                print('\n')
                time.sleep(5)
                session.close()
                session = get_session()
                try:
                    return should_go_ahead(url, session, string_to_check, timeout)
                except RecursionError:
                    print('Error 503, enough of recursion.')
                    print('\n')
                    go_ahead = False
            else:
                go_ahead = False
                print('No reason found for -', url)
                print('\n')
    except (Timeout, ChunkedEncodingError, ConnectionError) as e:
        print(e)
        print('Timeout, Sleeping for 5 sec...')
        print('\n')
        time.sleep(5)
        session.close()
        session = get_session()
        try:
            return should_go_ahead(url, session, string_to_check, timeout)
        except RecursionError:
            print('Timeout, enough of recursion.')
            print('\n')
            go_ahead = False

    return go_ahead, session, html_content


def parallelize_scraping(items, func, n_cores=config['algo']['vCPU']):
    if type(items) == list:
        df_titles = pd.DataFrame(items).rename(columns={0:'titles'})
        df_split = np.array_split(df_titles, n_cores)
    else:
        df_split = np.array_split(items, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def keep_connection_alive_for_scraping():
    start_time = datetime.now()
    keep_alive = True
    while keep_alive:
        time_since_start = (datetime.now() - start_time).seconds
        if time_since_start < 60:
            time_since_start = str(time_since_start) + ' seconds'
        elif time_since_start < 3600:
            time_since_start = str(time_since_start // 60) + ':' + str(time_since_start % 60) + ' minutes'
        else:
            time_since_start = str(time_since_start // 3600) + ':' + str((time_since_start % 3600) // 60) + ' hours'

        print('Keeping connection alive after '+time_since_start+'...')

        time.sleep(30)
        try:
            files = os.listdir('/home/ec2-user/scraped/')
        except FileNotFoundError:
            files = []
        if files:
            keep_alive = False

    return True


def launch_spot_instance(size='small'):
    session = boto3.Session(
        aws_access_key_id=config['s3']['aws_access_key_id'],
        aws_secret_access_key=config['s3']['aws_secret_access_key'],
        region_name=config['s3']['region_name']
    )
    client = session.client('ec2')

    print('Submitting fleet request...')
    if size == 'big':
        response = client.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::772835535876:role/aws-ec2-spot-fleet-tagging-role",
                "AllocationStrategy": "capacityOptimized",
                "TargetCapacity": 1,
                "TerminateInstancesWithExpiration": True,
                "LaunchSpecifications": [],
                "Type": "request",
                "LaunchTemplateConfigs": [
                    {
                        "LaunchTemplateSpecification": {
                            "LaunchTemplateId": "lt-0801fa586840fa707",
                            "Version": "4"
                        },
                        "Overrides": [
                            {
                                "InstanceType": "m5dn.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5d.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5a.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5ad.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5a.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5d.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5d.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5a.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5d.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5n.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5dn.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5n.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5d.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r5.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5ad.24xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5.metal",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            }
                        ]
                    }
                ]
            }
        )
    elif size == 'medium':
        response = client.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::772835535876:role/aws-ec2-spot-fleet-tagging-role",
                "AllocationStrategy": "capacityOptimized",
                "TargetCapacity": 1,
                "TerminateInstancesWithExpiration": True,
                "LaunchSpecifications": [],
                "Type": "request",
                "LaunchTemplateConfigs": [
                    {
                        "LaunchTemplateSpecification": {
                            "LaunchTemplateId": "lt-0801fa586840fa707",
                            "Version": "4"
                        },
                        "Overrides": [
                            {
                                "InstanceType": "m5n.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m5n.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "m5n.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "c5.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "c5.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "c5n.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5n.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "c5n.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "c4.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c4.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "c4.4xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            }
                        ]
                    }
                ]
            }
        )
    elif size == 'smallest':
        response = client.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::772835535876:role/aws-ec2-spot-fleet-tagging-role",
                "AllocationStrategy": "capacityOptimized",
                "TargetCapacity": 1,
                "TerminateInstancesWithExpiration": True,
                "LaunchSpecifications": [],
                "Type": "request",
                "LaunchTemplateConfigs": [
                    {
                        "LaunchTemplateSpecification": {
                            "LaunchTemplateId": "lt-0801fa586840fa707",
                            "Version": "4"
                        },
                        "Overrides": [
                            {
                                "InstanceType": "t3.nano",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "t3.nano",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "t3.nano",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "t3a.nano",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "t3a.nano",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "t3a.nano",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "t3a.micro",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "t3a.micro",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "t3a.micro",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "t3.micro",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "t3.micro",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "t3.micro",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "t3a.small",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "t3a.small",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "t3a.small",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "t3.small",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "t3.small",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "t3.small",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            }
                        ]
                    }
                ]
            }
        )
    else:
        response = client.request_spot_fleet(
            SpotFleetRequestConfig={
                "IamFleetRole": "arn:aws:iam::772835535876:role/aws-ec2-spot-fleet-tagging-role",
                "AllocationStrategy": "capacityOptimized",
                "TargetCapacity": 1,
                "TerminateInstancesWithExpiration": True,
                "LaunchSpecifications": [],
                "Type": "request",
                "LaunchTemplateConfigs": [
                    {
                        "LaunchTemplateSpecification": {
                            "LaunchTemplateId": "lt-0801fa586840fa707",
                            "Version": "4"
                        },
                        "Overrides": [
                            {
                                "InstanceType": "c5d.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "c5d.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5d.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "m4.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "m4.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "m4.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "c5n.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "c5n.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c5n.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "r3.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "r3.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "r3.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "c4.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "c4.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "c4.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            },
                            {
                                "InstanceType": "a1.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-aff487d5"
                            },
                            {
                                "InstanceType": "a1.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-6ec3c606"
                            },
                            {
                                "InstanceType": "a1.xlarge",
                                "WeightedCapacity": 1,
                                "SubnetId": "subnet-49ae7405"
                            }
                        ]
                    }
                ]
            }
        )
    spot_fleet_request_id = response['SpotFleetRequestId']
    print('Fleet request id -', spot_fleet_request_id)

    print('Fetching instances...')
    response = client.describe_spot_fleet_instances(
        SpotFleetRequestId=spot_fleet_request_id
    )
    while len(response['ActiveInstances']) == 0:
        time.sleep(5)
        print('Fetching instances again...')
        response = client.describe_spot_fleet_instances(
            SpotFleetRequestId=spot_fleet_request_id
        )
    instance_id = response['ActiveInstances'][0]['InstanceId']
    print('Instance id -', instance_id)

    print('Fetching instance public dns...')
    response = client.describe_instances(
        InstanceIds=[instance_id]
    )
    public_dns = response['Reservations'][0]['Instances'][0]['PublicDnsName']
    private_ip = response['Reservations'][0]['Instances'][0]['PrivateIpAddress']
    
    return spot_fleet_request_id, public_dns, private_ip


def close_spot_fleet_request_and_instances(spot_fleet_request_id):
    session = boto3.Session(
        aws_access_key_id=config['s3']['aws_access_key_id'],
        aws_secret_access_key=config['s3']['aws_secret_access_key'],
        region_name=config['s3']['region_name']
    )
    client = session.client('ec2')
    
    print('Cancelling fleet request & terminating instances...')
    client.cancel_spot_fleet_requests(
        SpotFleetRequestIds=[spot_fleet_request_id],
        TerminateInstances=True
    )
    
    return True


def get_active_spot_fleet_requests_count():
    session = boto3.Session(
        aws_access_key_id=config['s3']['aws_access_key_id'],
        aws_secret_access_key=config['s3']['aws_secret_access_key'],
        region_name=config['s3']['region_name']
    )
    client = session.client('ec2')

    response = client.describe_spot_fleet_requests(
        SpotFleetRequestIds=[]
    )

    counter = 0
    for request in response['SpotFleetRequestConfigs']:
        if request['SpotFleetRequestState'] in ['submitted', 'active']:
            counter += 1

    next_token = response.get('NextToken')
    while next_token:
        response = client.describe_spot_fleet_requests(
            NextToken=next_token
        )
        for request in response['SpotFleetRequestConfigs']:
            if request['SpotFleetRequestState'] in ['submitted', 'active']:
                counter += 1
        next_token = response.get('NextToken')

    return counter


def ssh_into_remote(hostname, username, key_file):
    client = None
    while client is None:
        try:
            print('Trying to ssh...')
            key = paramiko.RSAKey.from_private_key_file(key_file)
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            client.connect(hostname=hostname, username=username, pkey=key)
        except:
            print('Remote not completely up yet, sleeping for 10 sec...')
            time.sleep(10)
            client = None
    return client


def install_requirements_on_remote(public_dns, private_ip, username, key_file):
    default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)

    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=60, display=True) as interact:
        interact.expect(default_prompt)

        interact.send('sudo yum install htop')
        interact.expect('Is this ok \[y/d/N\]:\s+')
        interact.send('y')
        interact.expect(default_prompt)

        interact.send('sudo yum install python36 python36-pip')
        interact.expect('Is this ok \[y/d/N\]:\s+')
        interact.send('y')
        interact.expect(default_prompt)

        interact.send('sudo pip-3.6 install virtualenv')
        interact.expect(default_prompt)

        interact.send('sudo python3.6 -m virtualenv venv_data_collection')
        interact.expect(default_prompt)

        interact.send('source ./venv_data_collection/bin/activate')
        interact.expect('\(venv_data_collection\)\s+'+default_prompt)

        interact.send('sudo pip install --upgrade pip')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('sudo yum install python36-devel')
        interact.expect('Is this ok \[y/d/N\]:\s+')
        interact.send('y')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('sudo yum  install libevent-devel')
        interact.expect('Is this ok \[y/d/N\]:\s+')
        interact.send('y')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('sudo yum -y install gcc')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('sudo yum install git')
        interact.expect('Is this ok \[y/d/N\]:\s+')
        interact.send('y')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('git clone https://github.com/flibo-tech/data_pipelines.git')
        interact.expect("Username for 'https://github.com':\s+")
        interact.send(config['git']['username'])
        interact.expect("Password for 'https://"+config['git']['username']+"@github.com':\s+")
        interact.send(config['git']['password'])
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('cd data_pipelines')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'data_pipelines'))

        interact.send('git checkout develop')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'data_pipelines'))

        interact.send('sudo pip-3.6 install -r requirements.txt')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'data_pipelines'))

        client.close()
        return True


def scrape_on_remote(public_dns, private_ip, username, key_file, arg, index):
    default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)

    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=60*60, display=True) as interact:
        interact.expect(default_prompt)

        interact.send('source ./venv_data_collection/bin/activate')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('mkdir /home/' + username + '/scraped')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('cd data_pipelines/scraping_utilities')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'scraping_utilities'))

        interact.send('sudo python3.6 scrape.py ' + arg + ' ' + index)
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'scraping_utilities'))

        interact.send('sudo chmod -R 777 /home/' + username + '/scraped/')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'scraping_utilities'))

        client.close()
        return True


def collect_streaming_info_url_combos(public_dns, private_ip, username, key_file):
    default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)

    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=60*60, display=True) as interact:
        interact.expect(default_prompt)

        interact.send('source ./venv_data_collection/bin/activate')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('mkdir /home/' + username + '/scraped')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        print('\nTransferring RSA key to spot instance...')
        cmd = 'scp -r -o StrictHostKeyChecking=no -i ' + key_file + ' ' + key_file + ' ec2-user@' + public_dns + ':/tmp/key.pem'
        os.system(cmd)

        interact.send('cd data_pipelines/scraping_utilities/streaming_sources')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'streaming_sources'))

        interact.send('sudo python3.6 collect_streaming_urls_combos.py')
        try:
            interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'streaming_sources'), timeout=15*60)
        except:
            print('Waiting for query to end (in next step)...')

        client.close()

        client = ssh_into_remote(public_dns, username, key_file)
        with SSHClientInteraction(client, timeout=10 * 60, display=True) as interact:
            default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)
            interact.expect(default_prompt)

            interact.send('cd /home/ec2-user/scraped/')
            interact.expect(default_prompt.replace('~', 'scraped'))

            keep_checking = True
            while keep_checking:
                interact.send('ls')
                interact.expect(default_prompt.replace('~', 'scraped'))
                output = interact.current_output_clean
                if output.count('streaming_url_combos.csv') != 0:
                    interact.send('cat streaming_url_combos.csv | wc -l')
                    interact.expect(default_prompt.replace('~', 'scraped'))
                    combo_count = int(interact.current_output_clean)-1
                    keep_checking = False
                else:
                    print('Sleeping for 2 min...')
                    time.sleep(2*60)

            print('\nUploading file streaming_url_combos.csv to prod server...')

            interact.send('sudo chmod -R 777 /home/' + username + '/scraped/')
            interact.expect(default_prompt.replace('~', 'scraped'))

            interact.send(
                'sudo scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem /home/ec2-user/scraped/streaming_url_combos.csv ec2-user@' +
                config['ec2']['public_dns'] + ':/tmp/streaming_url_combos.csv')
            interact.expect(default_prompt.replace('~', 'scraped'))

        client.close()
        return combo_count


def collect_streaming_urls(public_dns, private_ip, username, key_file, index):
    default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)

    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=60*60, display=True) as interact:
        interact.expect(default_prompt)

        print('\nTransferring RSA key to spot instance...')
        cmd = 'scp -r -o StrictHostKeyChecking=no -i ' + key_file + ' ' + key_file + ' ec2-user@' + public_dns + ':/tmp/key.pem'
        os.system(cmd)

        print('\nTransferring streaming_url_combos.csv to spot instance...')
        interact.send('sudo scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem ec2-user@' + config['ec2'][
            'public_dns'] + ':/tmp/streaming_url_combos.csv /tmp/streaming_url_combos.csv')
        interact.expect(default_prompt)

        interact.send('source ./venv_data_collection/bin/activate')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('mkdir /home/' + username + '/scraped')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('cd data_pipelines/scraping_utilities/streaming_sources')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'streaming_sources'))

        interact.send('sudo python3.6 collect_streaming_urls.py '+index)
        try:
            interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'streaming_sources'), timeout=15*60)
        except:
            print('Waiting for query to end (in next step)...')

        client.close()

        client = ssh_into_remote(public_dns, username, key_file)
        with SSHClientInteraction(client, timeout=10 * 60, display=True) as interact:
            default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)
            interact.expect(default_prompt)

            interact.send('cd /home/ec2-user/scraped/')
            interact.expect(default_prompt.replace('~', 'scraped'))

            keep_checking = True
            while keep_checking:
                interact.send('ls')
                interact.expect(default_prompt.replace('~', 'scraped'))
                output = interact.current_output_clean
                if output.count('streaming_urls.csv') != 0:
                    keep_checking = False
                else:
                    print('Sleeping for 2 min...')
                    time.sleep(2*60)

            print('\nUploading file streaming_urls.csv to prod server...')

            interact.send('sudo chmod -R 777 /home/' + username + '/scraped/')
            interact.expect(default_prompt.replace('~', 'scraped'))

            interact.send(
                'sudo scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem /home/ec2-user/scraped/streaming_urls.csv ec2-user@' +
                config['ec2']['public_dns'] + ':/tmp/streaming_urls_'+index+'.csv')
            interact.expect(default_prompt.replace('~', 'scraped'))

        client.close()
        return True


def collate_streaming_urls(public_dns, private_ip, username, key_file, count):
    default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)

    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=60*60, display=True) as interact:
        interact.expect(default_prompt)

        max_spot_instances = config['scrape_data']['max_spot_instances']
        limit = count // max_spot_instances + (1 if count % max_spot_instances else 0)
        index_ranges = []
        for i in range(max_spot_instances):
            index_ranges.append(str(i * limit) + '-' + str(min(limit * i + limit, count)))

        print('\nTransferring RSA key to spot instance...')
        cmd = 'scp -r -o StrictHostKeyChecking=no -i ' + key_file + ' ' + key_file + ' ec2-user@' + public_dns + ':/tmp/key.pem'
        os.system(cmd)

        print('\nTransferring streaming_urls.csv to spot instance...')
        for index in index_ranges:
            interact.send('sudo scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem ec2-user@' + config['ec2'][
                'public_dns'] + ':/tmp/streaming_urls_'+index+'.csv /tmp/streaming_urls_'+index+'.csv')
            interact.expect(default_prompt)

        interact.send('source ./venv_data_collection/bin/activate')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('cd data_pipelines/scraping_utilities/streaming_sources')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'streaming_sources'))

        interact.send('sudo python3.6 collate_streaming_urls.py')
        try:
            interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'streaming_sources'), timeout=15*60)
        except:
            print('Waiting for query to end (in next step)...')

        client.close()

        client = ssh_into_remote(public_dns, username, key_file)
        with SSHClientInteraction(client, timeout=10 * 60, display=True) as interact:
            default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)
            interact.expect(default_prompt)

            interact.send('cd /home/ec2-user/scraped/')
            interact.expect(default_prompt.replace('~', 'scraped'))

            keep_checking = True
            while keep_checking:
                interact.send('ls')
                interact.expect(default_prompt.replace('~', 'scraped'))
                output = interact.current_output_clean
                if output.count('final_streaming_urls.csv') != 0:
                    interact.send('cat final_streaming_urls.csv | wc -l')
                    interact.expect(default_prompt.replace('~', 'scraped'))
                    urls_count = int(interact.current_output_clean) - 1
                    keep_checking = False
                else:
                    print('Sleeping for 2 min...')
                    time.sleep(2*60)

            print('\nUploading file final_streaming_urls.csv to prod server...')

            interact.send('sudo chmod -R 777 /home/' + username + '/scraped/')
            interact.expect(default_prompt.replace('~', 'scraped'))

            interact.send(
                'sudo scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem /home/ec2-user/scraped/final_streaming_urls.csv ec2-user@' +
                config['ec2']['public_dns'] + ':/tmp/streaming_urls.csv')
            interact.expect(default_prompt.replace('~', 'scraped'))

            print('Dumping content details data into CSV on prod...')
            engine = sqlalchemy.create_engine(
                'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
                    'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])

            con = engine.connect()
            trans = con.begin()
            sql_script = """copy (select imdb_content_id,
                                         title,
                                         release_year,
                                         case when type = 'tv' then 'show' else type end as item_type
                                  From app.content_details
                                  order by num_votes desc)
                            To '/tmp/content_metainfo.csv' WITH CSV DELIMITER '^' HEADER;
                         """
            con.execute(sql_script)
            trans.commit()
            con.close()

        client.close()
        return urls_count


def scrape_streaming_info_on_remote(public_dns, private_ip, username, key_file, index):
    default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)

    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=60*60, display=True) as interact:
        interact.expect(default_prompt)

        print('\nTransferring RSA key to spot instance...')
        cmd = 'scp -r -o StrictHostKeyChecking=no -i ' + key_file + ' ' + key_file + ' ec2-user@' + public_dns + ':/tmp/key.pem'
        os.system(cmd)

        print('\nTransferring streaming_urls.csv & content_metainfo.csv to spot instance...')
        interact.send('sudo scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem ec2-user@' + config['ec2'][
            'public_dns'] + ':/tmp/streaming_urls.csv /tmp/streaming_urls.csv')
        interact.expect(default_prompt)

        interact.send('sudo scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem ec2-user@' + config['ec2'][
            'public_dns'] + ':/tmp/content_metainfo.csv /tmp/content_metainfo.csv')
        interact.expect(default_prompt)

        interact.send('source ./venv_data_collection/bin/activate')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('mkdir /home/' + username + '/scraped')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('cd data_pipelines/scraping_utilities/streaming_sources')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'streaming_sources'))

        interact.send('sudo python3.6 scrape_streaming_info.py '+index)
        try:
            interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'streaming_sources'), timeout=60*60)
        except:
            print('Waiting for query to end (in next step)...')

        client.close()

        client = ssh_into_remote(public_dns, username, key_file)
        with SSHClientInteraction(client, timeout=10 * 60, display=True) as interact:
            default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)
            interact.expect(default_prompt)

            interact.send('cd /home/ec2-user/scraped/')
            interact.expect(default_prompt.replace('~', 'scraped'))

            keep_checking = True
            while keep_checking:
                interact.send('ls')
                interact.expect(default_prompt.replace('~', 'scraped'))
                output = interact.current_output_clean
                if output.count('streaming_info.csv') != 0:
                    keep_checking = False
                else:
                    print('Sleeping for 2 min...')
                    time.sleep(2*60)

            print('\nUploading file streaming_info.csv to prod server...')

            interact.send('sudo chmod -R 777 /home/' + username + '/scraped/')
            interact.expect(default_prompt.replace('~', 'scraped'))

            interact.send(
                'sudo scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem /home/ec2-user/scraped/streaming_info.csv ec2-user@' +
                config['ec2']['public_dns'] + ':/tmp/streaming_info_'+index+'.csv')
            interact.expect(default_prompt.replace('~', 'scraped'))

        client.close()
        return True


def collate_streaming_info(public_dns, private_ip, username, key_file, count):
    default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)

    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=60*60, display=True) as interact:
        interact.expect(default_prompt)

        max_spot_instances = config['scrape_data']['max_spot_instances']
        limit = count // max_spot_instances + (1 if count % max_spot_instances else 0)
        index_ranges = []
        for i in range(max_spot_instances):
            index_ranges.append(str(i * limit) + '-' + str(min(limit * i + limit, count)))

        print('\nTransferring RSA key to spot instance...')
        cmd = 'scp -r -o StrictHostKeyChecking=no -i ' + key_file + ' ' + key_file + ' ec2-user@' + public_dns + ':/tmp/key.pem'
        os.system(cmd)

        print('\nTransferring streaming_info CSVs to spot instance...')
        for index in index_ranges:
            interact.send('sudo scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem ec2-user@' + config['ec2'][
                'public_dns'] + ':/tmp/streaming_info_'+index+'.csv /tmp/streaming_info_'+index+'.csv')
            interact.expect(default_prompt)

        interact.send('source ./venv_data_collection/bin/activate')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('cd data_pipelines/scraping_utilities/streaming_sources')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'streaming_sources'))

        interact.send('sudo python3.6 collate_streaming_info.py')
        try:
            interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'streaming_sources'), timeout=60*60)
        except:
            print('Waiting for query to end (in next step)...')

        client.close()

        client = ssh_into_remote(public_dns, username, key_file)
        with SSHClientInteraction(client, timeout=10 * 60, display=True) as interact:
            default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)
            interact.expect(default_prompt)

            interact.send('cd /home/ec2-user/scraped/')
            interact.expect(default_prompt.replace('~', 'scraped'))

            keep_checking = True
            while keep_checking:
                interact.send('ls')
                interact.expect(default_prompt.replace('~', 'scraped'))
                output = interact.current_output_clean
                if output.count('final_streaming_info.csv') != 0:
                    keep_checking = False
                else:
                    print('Sleeping for 2 min...')
                    time.sleep(2*60)

            print('\nUploading file final_streaming_info.csv to prod server...')

            interact.send('sudo chmod -R 777 /home/' + username + '/scraped/')
            interact.expect(default_prompt.replace('~', 'scraped'))

            interact.send(
                'sudo scp -r -o StrictHostKeyChecking=no -i /tmp/key.pem /home/ec2-user/scraped/final_streaming_info.csv ec2-user@' +
                config['ec2']['public_dns'] + ':/tmp/streaming_info.csv')
            interact.expect(default_prompt.replace('~', 'scraped'))

        client.close()
        return True


def trigger_scrape_using_spot_instances(count, arg, limit_calc=False, skip_active_req_check=False):
    max_spot_instances = config['scrape_data']['max_spot_instances']
    if limit_calc:
        limit = count // max_spot_instances + (1 if count % max_spot_instances else 0)
        index_ranges = []
        for i in range(max_spot_instances):
            index_ranges.append(str(i * limit) + '-' + str(min(limit * i + limit, count)))
    else:
        limit = config['scrape_data']['crawls_per_spot_instance']
        index_ranges = []
        for i in range((count // limit) + (1 if count % limit else 0)):
            index_ranges.append(str(i * limit) + '-' + str(limit * i + limit))

    i = 0
    while index_ranges:
        to_scrape_on = index_ranges[:max_spot_instances]
        for index_range in to_scrape_on:
            if skip_active_req_check or (get_active_spot_fleet_requests_count() < max_spot_instances):
                i += 1
                print('Triggering scrape for index', index_range)
                if arg in ['collect_streaming_urls_using_spot_instance', 'scrape_streaming_urls_using_spot_instance']:
                    os.system('start "Scraping for index ' + index_range + '" cmd /k "' + config[
                        'venv_path'] + 'python" scrape.py ' + arg + ' ' + str(i) + ' ' + index_range)
                else:
                    os.system('start "Scraping for index ' + index_range + '" cmd /k "' + config[
                        'venv_path'] + 'python" scrape.py ' + arg + ' ' + index_range)

                index_ranges.remove(index_range)
            else:
                break
        print('\nRemaining instances to be triggered -', len(index_ranges))
        if index_ranges:
            print('Sleeping for 1 minute, will check then if we can launch more spot instances.\n')
            time.sleep(60)
    print('\n\nRequired number of spot instances launched. Check progress in any open terminals.')

    return True


def get_imdb_titles(df_urls):
    scrape_start_time = datetime.now()
    i = 1
    j = 0

    session = get_session()
    df_titles = pd.DataFrame()

    for url_item in df_urls.to_dict(orient='records'):
        go_ahead, session, html_content = should_go_ahead(url_item['url'], session, 'lister-item mode-advanced', timeout=15)

        if go_ahead:
            try:
                soup = BeautifulSoup(html_content, 'html.parser')

                items = soup.findAll('div', class_='lister-item mode-advanced')
                details = []
                for item in items:
                    header = item.find('h3', class_='lister-item-header')
                    title = header.find('a').text.strip()
                    imdb_id = header.find('a')['href'].split('/')[2]

                    try:
                        title_poster = item.find('img')['loadlate']
                        if title_poster.count('images/nopicture') == 0:
                            title_poster = title_poster.split('._V1_')[0] + '._V1_UY450.' + title_poster.split('.')[-1]
                        else:
                            title_poster = None
                    except:
                        title_poster = None

                    try:
                        imdb_score = eval(item.find('div', class_='inline-block ratings-imdb-rating').find('strong').text.strip())
                    except:
                        imdb_score = None

                    try:
                        metascore = eval(item.find('div', class_='inline-block ratings-metascore').find('span').text.strip())
                    except:
                        metascore = None

                    try:
                        block_items = item.find('p', class_='sort-num_votes-visible').findAll('span')
                        for block_item in block_items:
                            if block_item.get('name') == 'nv':
                                votes = eval(block_item.text.replace(',', '').strip())
                                break
                    except:
                        votes = None

                    details.append({
                        'imdb_content_id': imdb_id,
                        'title': title,
                        'poster': title_poster,
                        'imdb_score': imdb_score,
                        'metascore': metascore,
                        'votes': votes,
                        'type': url_item['type']
                    })

                df_titles = pd.concat([df_titles, pd.DataFrame(details)], axis=0)
            except Exception as e:
                print('Skipping', url_item['url'], '-', e)
                print('\n')
                j += 1
        else:
            print('Skipping', url_item['url'], '- something wrong.')
            print('\n')
            j += 1
        if i%5 == 0:
            print('URLs scraped -',i)

            time_since_start = (datetime.now()-scrape_start_time).seconds
            all_time_scraping_speed = (i/time_since_start)*3600
            if time_since_start < 60:
                time_since_start = str(time_since_start)+' seconds'
            elif time_since_start < 3600:
                time_since_start = str(time_since_start//60)+ ':'+str(time_since_start%60)+' minutes'
            else:
                time_since_start = str(time_since_start//3600)+ ':'+str((time_since_start%3600)//60)+' hours'
            print('Time since scraping started - '+time_since_start)
            print('All time scraping speed - '+('%.0f'%(all_time_scraping_speed))+' urls/hour')

            try:
                time_since_last_checkpoint = (datetime.now()-time_checkpoint).seconds
            except:
                time_since_last_checkpoint = (datetime.now()-scrape_start_time).seconds
            current_scraping_speed = (5/time_since_last_checkpoint)*3600
            time_remaining = (time_since_last_checkpoint*((df_urls.shape[0]-i-j)/5))/(3600)
            print('Current scraping speed - '+('%.0f'%(current_scraping_speed))+' urls/hour')
            print('Time remaining as per current speed - '+('%.1f'%(time_remaining))+' hours')
            print('\n')
            time_checkpoint = datetime.now()
        i += 1

    return df_titles


def collect_db_imdb_ids():
    engine = sqlalchemy.create_engine(
        'postgres://' + config['sql']['user'] + ':' + config['sql']['password'] + '@' + config['sql'][
            'host'] + ':' + str(config['sql']['port']) + '/' + config['sql']['db'])
    conn = engine.connect()

    df_db_ids = pd.read_sql("""
                                select distinct imdb_content_id
                                from """ + config['sql']['schema'] + """.content_details
                            """, con=conn)
    df_db_ids.to_csv('db_ids.csv', index=False)

    return True


def collect_new_imdb_ids():
    languages = config['scrape_data']['languages']

    links = []
    for content_type in ['feature', 'tv_series', 'tv_miniseries']:
        for language in languages:
            latest_release_date = config['scrape_data']['latest_release_date']
            start_year = int(latest_release_date.split('-')[0])

            today = date.today()
            this_year = today.year
            today = str(today.year) + '-' + str(today.month) + '-' + str(today.day)

            if language not in ['en', 'ja', 'es', 'fr', 'de', 'it', 'hi']:
                date_ranges = [{'start': latest_release_date, 'end': today}]
            else:
                years = []
                if language == 'en':
                    if start_year < 1990:
                        for i in range(int(np.ceil((1990 - start_year) / 10))):
                            if start_year >= 1990:
                                break
                            end_year = start_year + 10
                            end_year = end_year if end_year <= 1990 else 1990
                            years.append([start_year, end_year])
                            start_year = end_year
                    gap = 1
                else:
                    gap = 10

                for i in range(this_year - start_year):
                    if start_year == this_year:
                        break
                    end_year = start_year + gap
                    years.append([start_year, end_year])
                    start_year = end_year

                years.append([this_year, this_year])
                date_ranges = []
                for i in range(len(years)):
                    start = str(years[i][0]) + '-01-01'
                    end = str(years[i][1] - 1) + '-12-31'
                    if i == 0:
                        start = latest_release_date
                    if i == len(years) - 1:
                        end = today

                    date_ranges.append({
                        'start': start,
                        'end': end
                    })

            for date_range in date_ranges:
                print('\nPreparing links for - ', content_type, '+', language, '+', date_range['start'], '-', date_range['end'])
                url = 'https://www.imdb.com/search/title/?title_type=' + content_type + '&languages=' + language + '&release_date=' + \
                      date_range['start'] + ',' + date_range['end'] + '&sort=release_date,asc'
                session = get_session()
                html_content = session.get(url, timeout=5).text
                soup = BeautifulSoup(html_content, 'html.parser')
                check = soup.find('div', class_='lister-item mode-advanced')
                if check:
                    total = int(soup.find('div', class_='nav').find('div', class_='desc').find('span').text.split()[-2].replace(',', ''))
                    print(total, 'titles found.')
                    if total>10000:
                        print(url)
                    for i in range((total // 250) + (1 if total % 250 else 0)):
                        link = 'https://www.imdb.com/search/title/?title_type=' + content_type + '&languages=' + language + '&release_date=' + \
                               date_range['start'] + ',' + date_range['end'] + '&count=250&start=' + str(i*250 + 1) + '&sort=release_date,asc'
                        links.append({'url': link, 'type': 'movie' if content_type=='feature' else 'tv'})
                else:
                    print('No title found.')

    if links:
        df = pd.DataFrame(links).rename(columns={0: 'url'})
        df.to_csv('new_imdb_title_urls.csv', index=False)

        if config['scrape_data']['scrape_title_ids_on'] == 'local':
            print('\nStarting to scrape title ids...')
            df_titles = get_imdb_titles(df)
            df_titles.drop_duplicates(inplace=True)
            if config['scrape_data']['refresh_imdb_meta_info']:
                for col in ['metascore', 'votes']:
                    df_titles[col][pd.notnull(df_titles[col])] = df_titles[col][
                        pd.notnull(df_titles[col])].apply(lambda x: eval(str(x).replace(',', '')))
                    df_titles[col][pd.notnull(df_titles[col])] = df_titles[col][
                        pd.notnull(df_titles[col])].apply(lambda x: '{:.0f}'.format(x))

                df_titles.to_csv(config['to_upload']+ 'content_meta_info.csv', index=False, sep='^')
            else:
                df_titles.to_csv('new_imdb_titles.csv', index=False)
            print('Title ids scraped.')
        elif config['scrape_data']['scrape_title_ids_on'] == 'remote':
            print('\nFile new_imdb_title_urls.csv has been updated. Push it to GIT to proceed.')
            time.sleep(1)
            check = input('Have you pushed it to GIT? (y/n)')
            while check != 'y':
                print('\nYou have to push the file to GIT to proceed.')
                time.sleep(1)
                check = input('Have you pushed it to GIT? (y/n)')

            print('\nStarting...')
            count = pd.read_csv('new_imdb_title_urls.csv').shape[0]
            trigger_scrape_using_spot_instances(count, 'operate_spot_instance_to_scrape_title_ids')
    else:
        print('\nNo new title found for any language.')

    return True
