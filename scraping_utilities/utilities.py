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


def parallelize_scraping(titles, func, n_cores=config['algo']['vCPU']):
    df_titles = pd.DataFrame(titles).rename(columns={0:'titles'})
    df_split = np.array_split(df_titles, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def launch_spot_instance():
    session = boto3.Session(
        aws_access_key_id=config['s3']['aws_access_key_id'],
        aws_secret_access_key=config['s3']['aws_secret_access_key'],
        region_name=config['s3']['region_name']
    )
    client = session.client('ec2')

    print('Submitting fleet request...')
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

        interact.send('git checkout faster_imdb_scraping')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'data_pipelines'))

        interact.send('sudo pip-3.6 install -r requirements.txt')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'data_pipelines'))

        client.close()
        return True


def scrape_data_on_remote(public_dns, private_ip, username, key_file):
    default_prompt = '\[username@ip-private-ip ~\]\$\s+'.replace('private-ip', private_ip.replace('.', '-')).replace('username', username)

    client = ssh_into_remote(public_dns, username, key_file)
    with SSHClientInteraction(client, timeout=60*60, display=True) as interact:
        interact.expect(default_prompt)

        interact.send('source ./venv_data_collection/bin/activate')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt)

        interact.send('cd data_pipelines/scraping_utilities')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'scraping_utilities'))

        interact.send('sudo python3.6 scrape.py scrape_on_spot_instance')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'scraping_utilities'))

        interact.send('sudo cp /root/final_file.csv /home/'+username+'/')
        interact.send('sudo chmod 777 /home/' + username + '/final_file.csv')
        interact.expect('\(venv_data_collection\)\s+' + default_prompt.replace('~', 'scraping_utilities'))

        client.close()
        return True
