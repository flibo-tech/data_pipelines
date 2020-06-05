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
