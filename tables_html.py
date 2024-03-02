"""
парсинг таблиц из html
"""
import json

from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
import countries as c_countries
import config
import common

http_adapter = HTTPAdapter(max_retries=10)


def load_html(url):
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url, timeout=(100, 100))
    if r.ok:
        lws = BeautifulSoup(r.text, 'html.parser').\
            find_all('table', class_='stripe row-border order-column compact')[0].\
            find_all('tbody')[0].text.split('\n')
        i = len(lws) - 1
        while i >= 0:
            if lws[i].strip() == '':
                lws.pop(i)
            i -= 1
        return lws


def load_trading(url):
    apikey = 'guest:guest'
    # session = requests.Session()
    # session.mount(url, http_adapter)
    # r = session.get(url, timeout=(100, 100))
    # if r.ok:
    #     lws = BeautifulSoup(r.text, 'html.parser').\
    #         find_all('table', class_='stripe row-border order-column compact')[0].\
    #         find_all('tbody')[0].text.split('\n')
    # else:
    #     print(r.text)
    url = url + f'?c={apikey}'
    r = requests.get(url)
    data = r.text
    print(data)