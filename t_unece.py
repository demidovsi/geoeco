import trafaret_thread
import common
from common import SRC
import config
import json
import time
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

http_adapter = HTTPAdapter(max_retries=10)
obj = None


class Unece(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Unece, self).__init__(source, code_function)

    def work(self):
        super(Unece, self).work()
        return self.load_list_indicators()

    def check_import_metric(self):
        super(Unece, self).check_import_metric()
        if len(self.list_start_metric) == 0:
            return
        countries = common.load_countries(self.token)
        if countries is None:
            return
        answer = self.load_indicators(False)
        if answer is None:
            return
        for indicator in self.list_start_metric:
            t0 = time.time()
            found = False
            for data in answer:
                if data['param_name'] == indicator:
                    found = True
                    count_row, st_absent, max_year, new_rows = self.import_data(data, countries=countries)
                    self.decode_finish_one_indicator(data, count_row, st_absent, t0, new_rows)
            self.not_found_indicator(indicator, found)

    def import_data(self, data, countries):
        st_absent = ''
        url = self.par['compliment_txt']['url'].format(indicator=data['code'])
        lws = self.load_html(url)
        if lws is None:
            return 0, '', '', None
        heads = lws.find_all('thead')[0].find_all('td')
        years = list()
        for i in range(1, len(heads)):
            years.append(heads[i].text)
        trs = lws.find_all('tbody')[0].find_all('tr')
        st_query = ''
        list_country = list()
        for tr in trs:
            tds = tr.find_all('td')
            country_name = tds[0].text
            country_id = common.get_country_id(country_name, countries, pr=False)
            if country_id is None:
                if country_name not in st_absent:
                    st_absent = st_absent + ', ' if st_absent else st_absent
                    st_absent += country_name
                continue  # эту строку пропускаем
            for i in range(1, len(tds)):
                value = tds[i].text
                if common.is_number(value):
                    date = years[i - 1] + '-01-01'
                    if country_id not in list_country:
                        list_country.append(country_id)
                    st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});\n". \
                        format(date=date, country_id=country_id, value=value, schema=config.SCHEMA,
                               param_name=data['param_name'])
        is_ok, new_rows = common.write_his_db(st_query, 'his_countries_' + data['param_name'], self.token)
        max_year = max((y for y in years if y.strip().isdigit()), default='')
        return len(list_country), st_absent, max_year, new_rows

    def load_html(self, url):
        t = time.time()
        try:
            session = requests.Session()
            session.mount(url, http_adapter)
            r = session.get(url, timeout=(100, 100))
            if r.ok:
                lws = BeautifulSoup(r.text, 'html.parser').find_all('table')[0]
                return lws
            else:
                common.write_log_db('❌Error', SRC, 'load_html\n' + str(r.status_code) + '; ' + r.text,
                                    law_id= self.code_parser + '\n' + self.source, td=time.time() - t)
        except Exception as err:
            st = f"{err}\n" + url
            common.write_log_db('💥Exception', SRC, 'load_html\n' + st,
                                law_id=self.code_parser + '\n' + self.source, td=time.time() - t)

    def load_list_indicators(self):
        result = 0
        index = 0
        answer = self.load_indicators(True)
        if answer:
            countries = common.load_countries(self.token)
            if countries is None:
                print('Отсутствуют страны')
                return False
            for data in answer:
                index += 1
                t0 = time.time()
                count_row, st_absent, max_year, new_rows = self.import_data(data, countries)
                if count_row:
                    result += count_row
                    self.add_new_rows(new_rows)
                # записать результат разбора импорта по одному индикатору
                self.finish(count_row, st_absent, data, len(answer), index, time.time() - t0, max_year, new_rows)
        return result != 0

    def finish(self, count_row, st_absent, data, count, index, td, max_year, new_rows=None):
        own = f'\n{index}/{count}'
        if count_row:
            st = data['name_rus']
            if st_absent:
                st += ';\n не найдены страны: "' + st_absent + '"'
            common.write_log_db(
                '✔️import', SRC,
                st + '\n' + '(' + data['period'] + ') ' + 'param_name="' + data['param_name'] + '; ' + data['object_code'] + f'"\nmax_year={max_year}' + common.st_new_rows(new_rows),
                law_id=self.code_parser + '\n' + self.source + own, page=count_row, td=td)
        else:
            common.write_log_db(
                'Error', SRC, ' (' + data['period'] + ') - ' + 'Не обнаружено данных ' +
                "\nparam_name='" + data['param_name'] + '; ' + data['object_code'] + "'",
                law_id=self.code_parser + '\n' + self.source + own, td=td)


if __name__ == '__main__':
    Unece('ООН', 'un').start()
    while True:
        time.sleep(5)
