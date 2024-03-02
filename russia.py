from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from deep_translator import GoogleTranslator
import common
import provinces

http_adapter = HTTPAdapter(max_retries=10)


def load_html(url):
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url, timeout=(100, 100))
    if r.ok:
        lws = BeautifulSoup(r.text, 'html.parser').find_all('table')[0].find_all('tbody')[0].find_all('tr')[1:]
        return lws
    else:
        return []


def load_inform():
    country_id = 200
    values = list()
    lws = load_html("https://www.statdata.ru/spisok-regionov-rossii-s-kodamy")
    if len(lws) > 0:
        list_provinces = provinces.load_provinces_db(country_id)
        for unit in lws:
            tr = unit.find_all('td')
            data = dict()
            data['code'] = tr[0].text
            data['name_own'] = tr[1].text
            data['name_rus'] = data['name_own']
            data['sh_name'] = GoogleTranslator(source='ru', target='en').translate(data['name_own'])
            data['country'] = country_id
            for province in list_provinces:
                if data['name_own'] == province['name_own']:
                    data['id'] = province['id']
                    break
            values.append(data)
        common.write_objects_db('provinces', values)

