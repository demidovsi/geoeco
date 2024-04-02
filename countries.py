#https://w3.unece.org/PXWeb/ru/TableDomains/
import time
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
import json

import common
import config

http_adapter = HTTPAdapter(max_retries=10)
api_key = 'DFtDCCAiCZNZIEtAj1aa5VwJzIE8W8Ep'


def load_courses(st_date):
    url = 'v1/select/{schema}/nsi_currencies'.format(schema=config.SCHEMA)
    currencies, is_ok, status_response = common.send_rest(url, params={"columns": "id, code"})
    if not is_ok:
        print(str(currencies))
        return
    currencies = json.loads(currencies)
    list_currencies = ''
    for data in currencies:
        list_currencies = list_currencies + ',' if list_currencies else list_currencies
        list_currencies = list_currencies + data['code']

    url = "https://api.apilayer.com/exchangerates_data/" + st_date + "?base=USD"
    headers = {"apikey": api_key}
    # прочитать валюту с сайта
    response = requests.request('GET', url + "&symbols=" + list_currencies, headers=headers, timeout=300)
    if response.ok:
        status = json.loads(response.text)
        result = status['rates']
        st_dt = status['date']
        st_query = ''
        for unit in result:
            value = result[unit]
            st_query = st_query + '\n' if st_query != '' else st_query
            currency_id = None
            for data in currencies:
                if data['code'] == unit:
                    currency_id = data['id']
                    break
            if currency_id:
                st_query = st_query + "select {schema}.pw_his_currencies('{date}', '{currency_id}', {value});".format(
                    date=st_dt, currency_id=currency_id, value=1 / value, schema=config.SCHEMA
                )
        common.write_script_db(st_query)
    else:
        print(response.status_code, response.text)


def load_html(url, source):
    t = time.time()
    url = 'https://www.ixbt.com/mobile/country_code.html'
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url, timeout=(100, 100))
    try:
        if r.ok:
            return r.text
        else:
            common.write_log_db(
                'Error', source, str(r.status_code) + '; ' + r.text, td=time.time() - t,
                file_name=common.get_computer_name())
    except Exception as err:
        st = f"{err}\n" + url
        common.write_log_db('Exception', source, st, td=time.time() - t, file_name=common.get_computer_name())


def load_phone_code():
    ans, is_ok, token, lang = common.login_superadmin()
    if not is_ok:
        print('Error login')
        return str(ans)
    url = 'https://www.ixbt.com/mobile/country_code.html'
    lws = load_html(url, source='phone_code')
    if lws is None:
        return
    lws = BeautifulSoup(lws, 'html.parser').find_all('table')[8].find_all('tr')
    lws = lws[1:]
    countries = common.load_countries(token)
    st_query = ''
    for data in lws:
        unit = data.find_all('td')
        name = unit[0].text
        code = unit[2].text
        country_id = common.get_country_id(name, countries)
        if country_id:
            st_query += "update {schema}.nsi_countries set phone_code='{code}' where id={country_id}; select 1;\n". \
                format(schema=config.SCHEMA, code=code.strip(), country_id=country_id)
    common.write_script_db(st_query, token=token)

# load_phone_code()