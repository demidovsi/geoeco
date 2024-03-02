from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from deep_translator import GoogleTranslator
import common
import config
import countries as c_countries

http_adapter = HTTPAdapter(max_retries=10)


def load_html(url):
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url, timeout=(100, 100))
    if r.ok:
        lws = BeautifulSoup(r.text, 'html.parser').find_all('table', class_="wikitable sortable zebra")[0].\
            find_all('tbody')[0].find_all('tr')
        return lws
    else:
        print(r.status_code, r.text)


def get_corruption_index():
    url = "https://ru.wikipedia.org/wiki/%D0%A1%D0%BF%D0%B8%D1%81%D0%BE%D0%BA_%D1%81%D1%82%D1%80%D0%B0%D0%BD_" \
          "%D0%BF%D0%BE_%D0%B8%D0%BD%D0%B4%D0%B5%D0%BA%D1%81%D1%83_" \
          "%D0%B2%D0%BE%D1%81%D0%BF%D1%80%D0%B8%D1%8F%D1%82%D0%B8%D1%8F_" \
          "%D0%BA%D0%BE%D1%80%D1%80%D1%83%D0%BF%D1%86%D0%B8%D0%B8#:~:text=" \
          "%D0%98%D0%BD%D0%B4%D0%B5%D0%BA%D1%81%20%D0%B2%D0%BE%D1%81%D0%BF%D1%80%D0%B8%D1%8F%D1%82%D0%B8%D1%8F" \
          "%20%D0%BA%D0%BE%D1%80%D1%80%D1%83%D0%BF%D1%86%D0%B8%D0%B8%20(%D0%B0%D0%BD%D0%B3%D0%BB.," \
          "%D0%A1%D0%BE%D1%81%D1%82%D0%B0%D0%B2%D0%BB%D1%8F%D0%B5%D1%82%D1%81%D1%8F" \
          "%20%D0%B5%D0%B6%D0%B5%D0%B3%D0%BE%D0%B4%D0%BD%D0%BE%20%D1%81%201995%20%D0%B3%D0%BE%D0%B4%D0%B0."
    lws = load_html(url)
    if lws is None:
        return
    countries = c_countries.load_countries()
    if countries is None:
        print('Отсутствуют страны')
        return
    st_years = lws[1].text.strip().split('\n')
    years = list()
    for year in st_years:
        if year:
            years.append(year.strip().split('[')[0])
    lws = lws[2:]
    st_query = ''
    param_name = 'corruption_index'
    for data in lws:
        unit = data.find_all('td')
        country_id = common.get_country_id(unit[1].text, countries)
        if country_id:
            for i in range(2, len(unit)):
                if unit[i].text.strip() != 'Н/Д':
                    st_query += "select {schema}.pw_his('{param_name}', '{date}', {country_id}, {value});\n". \
                        format(param_name=param_name, date=years[i-2] +'-12-31', country_id=country_id,
                               value=unit[i].text.strip(), schema=config.SCHEMA)
    common.write_script_db(st_query)


