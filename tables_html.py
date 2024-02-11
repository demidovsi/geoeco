"""
парсинг таблиц из html
"""
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


def make_numbeo_crime(lws, year, param_name, count, i_name, i_value):
    """
    :param lws: массив колонок строк таблицы (без шапки)
    :param year: - год информации
    :param param_name: - имя параметра в БД
    :param count: - количество колонок в строке таблицы (без номера строки)
    :param i_name: - индекс колонки с именем страны в строке таблицы
    :param i_value:  - индекс колонки со значением в строке таблицы
    :return:
    """
    countries = c_countries.load_countries()
    if countries is None:
        return
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    if not is_ok:
        return
    date = '{year}-01-01'.format(year=year)
    print(date, len(lws))
    st_query = ''
    for i in range(0, len(lws), count):
        name = lws[i + i_name]
        value = lws[i + i_value]
        country_id = None
        name = name.replace(' (China)','').replace('Us', 'United States')
        for country in countries:
            if name.upper() in [country['sh_name'].upper(), country['official'].upper()]:
                country_id = country['id']
                break
        if country_id is None:
            print('absent', name)
            continue
        if value:
            st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});".format(
                date=date, country_id=country_id, value=value, schema=config.SCHEMA, param_name=param_name)
    answer, ok, status = common.send_rest(
        'v1/NSI/script/execute', 'PUT', st_query, lang='en', token_user=token)
    if not ok:
        print(answer)


def make_numbeo_salary(index, param_name):
    countries = c_countries.load_countries()
    if countries is None:
        return
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    if not is_ok:
        return
    date = common.st_today()
    session = requests.Session()
    url = config.url_numbeo_cost + str(index)
    session.mount(url, http_adapter)
    r = session.get(url, timeout=(100, 100))
    if r.ok:
        st_query = ''
        lws = BeautifulSoup(r.text, 'html.parser').\
            find_all('table', class_='stripe row-border order-column compact')[0].find_all('tbody')[0].find_all('tr')
        for row in lws:
            unit = row.find_all('td')
            name = unit[1].find('a').text
            value = unit[2].text
            country_id = None
            name = name.replace(' (China)', '').replace('Us', 'United States')
            for country in countries:
                if name.upper() in [country['sh_name'].upper(), country['official'].upper()]:
                    country_id = country['id']
                    break
            if country_id is None:
                print('absent', name)
                continue
            if value:
                st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});".format(
                    date=date, country_id=country_id, value=value, schema=config.SCHEMA, param_name=param_name)
        if st_query:
            answer, ok, status = common.send_rest(
                'v1/NSI/script/execute', 'PUT', st_query, lang='en', token_user=token)
            if not ok:
                print(answer)


def load_trading(url):
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url, timeout=(100, 100))
    if r.ok:
        lws = BeautifulSoup(r.text, 'html.parser').\
            find_all('table', class_='stripe row-border order-column compact')[0].\
            find_all('tbody')[0].text.split('\n')
    else:
        print(r.text)
