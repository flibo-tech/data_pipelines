from http_request_randomizer.requests.proxy.requestProxy import RequestProxy
import requests
import pandas as pd
import numpy as np
import yaml
from multiprocessing import Pool
from selenium.webdriver.remote.remote_connection import LOGGER, logging
from selenium import webdriver


LOGGER.setLevel(logging.WARNING)


config = yaml.safe_load(open('./../config.yml'))


def get_driver(proxy=None):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--window-size=800x800')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('log-level=3')
    if proxy:
        options.add_argument('--proxy-server=' + proxy)

    driver = webdriver.Chrome(chrome_options=options)

    return driver


def parallelize_validation(proxies, func, n_cores=config['algo']['vCPU']):
    df_proxies = pd.DataFrame(proxies).rename(columns={0: 'proxy'})
    df_split = np.array_split(df_proxies, n_cores)

    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


def validate_proxies(df_proxies):
    proxies = list(df_proxies['proxy'].unique())
    title_id = 'tt0111161' #The Shawshank Redemption

    valid_proxies = []
    for proxy in proxies:
        print('Validating proxy -', proxy)

        proxyDict = {
            "http": 'http://' + proxy,
            "https": 'https://' + proxy,
            "ftp": 'ftp://' + proxy
        }
        try:
            html_content = requests.get("http://www.imdb.com/title/" + title_id, proxies=proxyDict).text
            if html_content.count('title_wrapper') != 0:
                valid_proxies.append(proxy)
        except:
            pass

    df = pd.DataFrame(valid_proxies).rename(columns={0: 'valid_proxy'})

    return df


def get_proxies():
    proxies = []
    req_proxy = RequestProxy()
    for proxy in req_proxy.get_proxy_list():
        proxies.append(proxy.ip + ':' + str(proxy.port))

    proxies = list(set(proxies))
    print(len(proxies), 'proxies gathered.')
    print('Starting to validate proxies...')
    df_proxies = parallelize_validation(proxies, validate_proxies)
    proxies = list(df_proxies['valid_proxy'].unique())
    print('Remaining proxies after validation -', len(proxies))

    return proxies
