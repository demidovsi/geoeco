import trafaret_thread
import common
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
            for data in answer:
                if data['param_name'] == indicator:
                    count_row, st_absent = self.import_data(data, countries=countries)
                    self.decode_finish_one_indicator(data, count_row, st_absent, t0)

    def import_data(self, data, countries):
        st_absent = ''
        url = self.par['compliment_txt']['url'].format(indicator=data['code'])
        lws = self.load_html(url)
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
            if country_id is None and country_name not in st_absent:
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
        common.write_script_db(st_query, self.token)
        return len(list_country), st_absent

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
                count_row, st_absent = self.import_data(data, countries)
                if count_row:
                    result += count_row
                # записать результат разбора импорта по одному индикатору
                self.finish(count_row, st_absent, data, len(answer), index, time.time() - t0)
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


# Unece('ООН', 'un').start()
# while True:
#     time.sleep(5)
