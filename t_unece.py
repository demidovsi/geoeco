import trafaret_thread
import common
import config
import json
import time
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from deep_translator import GoogleTranslator

http_adapter = HTTPAdapter(max_retries=10)
obj = None


class Unece(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Unece, self).__init__(source, code_function)

    def work(self):
        super(Unece, self).work()
        return self.load_list_indicators()

    def load_html(self, url):
        t = time.time()
        session = requests.Session()
        session.mount(url, http_adapter)
        r = session.get(url, timeout=(100, 100))
        try:
            if r.ok:
                lws = BeautifulSoup(r.text, 'html.parser').find_all('table')[0]
                return lws
            else:
                common.write_log_db(
                    'Error', self.source, str(r.status_code) + '; ' + r.text, td=time.time() - t,
                    file_name=common.get_computer_name(), token_admin=self.token)
        except Exception as err:
            st = f"{err}\n" + url
            common.write_log_db(
                'Exception', self.source, st, td=time.time() - t,
                file_name=common.get_computer_name(), token_admin=self.token)

    def load_list_indicators(self):
        url = "v1/select/{schema}/nsi_import?where=sh_name='{code_function}' and active".format(
            schema=config.SCHEMA, code_function=self.code_parser)
        answer, is_ok, status = common.send_rest(url)
        result = 0
        index = 0
        if is_ok:
            answer = json.loads(answer)
            countries = common.load_countries(self.token)
            if countries is None:
                print('Отсутствуют страны')
                return False
            for data in answer:
                index += 1
                count_row = 0
                st_absent = ''
                t0 = time.time()
                url = self.par['compliment_txt']['url'].format(indicator=data['code'])
                lws = self.load_html(url)
                heads = lws.find_all('thead')[0].find_all('td')
                years = list()
                for i in range(1, len(heads)):
                    years.append(heads[i].text)
                trs = lws.find_all('tbody')[0].find_all('tr')
                st_query = ''
                count_countries = 0
                for tr in trs:
                    tds = tr.find_all('td')
                    country_name = tds[0].text
                    country_id = common.get_country_id(country_name, countries, pr=False)
                    if country_id is None and country_name not in st_absent:
                        st_absent = st_absent + ', ' if st_absent else st_absent
                        st_absent += country_name
                        continue  # эту строку пропускаем
                    for i in range(1, len(tds)):
                        value = tds[i].text
                        if common.is_number(value):
                            date = years[i - 1] + '-01-01'
                            count_row += 1
                            st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});\n".\
                                format(date=date, country_id=country_id, value=value, schema=config.SCHEMA,
                                       param_name=data['param_name'])
                    if count_row:
                        count_countries += 1  # количество стран
                        result += 1  # для выхода
                common.write_script_db(st_query, self.token)
                # записать результат разбора импорта по одному индикатору
                self.finish(count_countries, st_absent, data, len(answer), index, time.time() - t0)
        return result != 0

    def finish(self, count_row, st_absent, data, count, index, td):
        if count_row:
            st = data['name_rus']
            if st_absent:
                st += ';\n не найдены страны: "' + st_absent + '"'
            common.write_log_db(
                'import', self.source + ' (' + data['period'] + ')', st, page=count_row, td=td,
                law_id=str(index) + ' (всего=' + str(count) + ')',
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser +
                '"; param_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                token_admin=self.token)
        else:
            common.write_log_db(
                'Error', self.source + ' (' + data['period'] + ')', 'Не обнаружено данных', td=td,
                law_id=str(index) + ' (всего=' + str(count) + ')',
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser +
                '"; param_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                token_admin=self.token)

Unece('ООН', 'un').start()
while True:
    time.sleep(5)
