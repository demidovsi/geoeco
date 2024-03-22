import trafaret_thread
import common
import config
import json
import requests
from requests.adapters import HTTPAdapter

http_adapter = HTTPAdapter(max_retries=10)
api_key = 'DFtDCCAiCZNZIEtAj1aa5VwJzIE8W8Ep'
obj = None


class Courses(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Courses, self).__init__(source, code_function)

    def load_courses(self, st_date):
        count_row = 0
        url = 'v1/select/{schema}/nsi_currencies'.format(schema=config.SCHEMA)
        currencies, is_ok, status_response = common.send_rest(url)
        if not is_ok:
            common.write_log_db('ERROR', self.source, str(currencies), token_admin=self.token)
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
            if status['success']:
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
                        count_row += 1
                        st_query = st_query + "select {schema}.pw_his_currencies('{date}', '{currency_id}', {value});".format(
                            date=st_dt, currency_id=currency_id, value=1 / value, schema=config.SCHEMA
                        )
                self.finish_text = 'Загружено ' + str(count_row) + ' курсов валют'
                if not common.write_script_db(st_query, token=self.token):
                    count_row = 0
            else:
                common.write_log_db('ERROR', self.source, str(status), token_admin=self.token)
        else:
            common.write_log_db('ERROR', self.source,
                                str(response.status_code) + ': ' + response.text, token_admin=self.token)
        return count_row != 0

    def work(self):
        super(Courses, self).work()
        return self.load_courses(common.st_yesterday())
