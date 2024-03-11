#https://w3.unece.org/PXWeb/ru/TableDomains/
import time
import requests
from requests.adapters import HTTPAdapter
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
