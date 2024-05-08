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


def get_st_sql_cities(st_sql, date, city_id, value, param_name):
    st_sql += "select {schema}.pw_his_cities('{param_name}', '{date}', {city_id}, {value});\n". \
        format(param_name=param_name, date=date, city_id=city_id, value=value,
               schema=config.SCHEMA)
    return st_sql


class Numbeo(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Numbeo, self).__init__(source, code_function)

    def work(self):
        super(Numbeo, self).work()
        return self.load_list_indicators()

    def check_import_metric(self):
        super(Numbeo, self).check_import_metric()
        if len(self.list_start_metric) == 0:
            return
        countries = common.load_countries(self.token)
        if countries is None:
            return
        answer = self.load_indicators(False)
        if answer is None:
            return
        for indicator in self.list_start_metric:
            for data in answer:  # цикл по индикаторам
                t0 = time.time()
                if data['param_name'] == indicator:  # индикатор заказан
                    count_row = None
                    st_absent = ''
                    if '{year}' in data['code']:
                        if data['object_code'] == 'countries':
                            count_row, st_absent = self.make_years_countries(data, countries)
                        else:
                            count_row, st_absent = self.make_years_cities(data, countries)
                    elif data['object_code'] == 'countries':
                        count_row, st_absent = self.make_current_value(data['code'], data['param_name'], countries)
                    if count_row:
                        self.decode_finish_one_indicator(data, count_row, st_absent, t0)

    def load_list_indicators(self):
        result = 0
        index = 0
        answer = self.load_indicators(True)
        if answer:
            countries = common.load_countries()
            if countries is None:
                return False
            for data in answer:
                index += 1
                count_row = None
                st_absent = ''
                t0 = time.time()

                if '{year}' in data['code']:
                    if data['object_code'] == 'countries':
                        count_row, st_absent = self.make_years_countries(data, countries)
                    else:
                        count_row, st_absent = self.make_years_cities(data, countries)
                elif data['object_code'] == 'countries':
                    count_row, st_absent = self.make_current_value(data['code'], data['param_name'], countries)

                # записать результат разбора импорта по одному индикатору
                self.finish(count_row, st_absent, data, len(answer), index, time.time() - t0)
                if count_row:
                    result += count_row
        return result != 0

    def make_current_value(self, url, param_name, countries):
        # месячная информация по странам для одного параметра (текущий месяц)
        date = common.st_month()
        session = requests.Session()
        session.mount(url, http_adapter)
        r = session.get(url, timeout=(100, 100))
        if r.ok:
            st_query = ''
            st_absent = ''
            count_row = 0
            lws = BeautifulSoup(r.text, 'html.parser').\
                find_all('table', class_='stripe row-border order-column compact')[0].\
                find_all('tbody')[0].\
                find_all('tr')
            for row in lws:
                unit = row.find_all('td')
                name = unit[1].find('a').text
                value = unit[2].text
                country_id = common.get_country_id(name, countries, pr=False)
                if country_id is None and name not in st_absent:
                    st_absent = st_absent + ', ' if st_absent else st_absent
                    st_absent += name
                    continue
                if value:
                    count_row += 1
                    st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});".format(
                        date=date, country_id=country_id, value=value, schema=config.SCHEMA, param_name=param_name)
            common.write_script_db(st_query, self.token)
            return count_row, st_absent
        else:
            return None, ''

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

    def load_html(self, url):
        t = time.time()
        session = requests.Session()
        session.mount(url, http_adapter)
        r = session.get(url, timeout=(100, 100))
        try:
            if r.ok:
                lws = BeautifulSoup(r.text, 'html.parser'). \
                    find_all('table', class_='stripe row-border order-column compact')[0]. \
                    find_all('tbody')[0].text.split('\n')
                i = len(lws) - 1
                while i >= 0:
                    if lws[i].strip() == '':
                        lws.pop(i)
                    i -= 1
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

    def make_years_countries(self, data, countries):
        # годовая информация по странам для одного параметра (за интервал годов с 2012-го)
        st_absent = ''
        count_row = 0
        for year in range(2012, time.gmtime().tm_year + 1):
            url = data['code'].format(year=year)
            lws = self.load_html(url)
            if lws:
                date = '{year}-01-01'.format(year=year)
                st_query = ''
                for i in range(0, len(lws), data['column_count']):
                    name = lws[i + data['ind_name']]
                    value = lws[i + data['ind_value']]
                    country_id = common.get_country_id(name, countries, pr=False)
                    if country_id is None and name not in st_absent:
                        st_absent = st_absent + ', ' if st_absent else st_absent
                        st_absent += name
                        continue  # эту строку пропускаем
                    if value and value != '-':
                        count_row += 1
                        st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});".format(
                            date=date, country_id=country_id, value=value, schema=config.SCHEMA,
                            param_name=data['param_name'])
                common.write_script_db(st_query, self.token)
        return count_row, st_absent

    def make_years_cities(self, data, countries):
        # годовая информация по странам для одного параметра (за интервал годов с 2012-го)
        cities = common.load_cities(self.token)
        if cities is None:
            return None, ''
        count_row = 0
        for year in range(2012, time.gmtime().tm_year + 1):
            lws = self.load_html(data['code'].format(year=year))
            if lws:
                date = '{year}-01-01'.format(year=year)
                # print(date, len(lws))
                st_sql = ''
                need_reload = False
                # проверим и запишем новые города
                for i in range(0, len(lws), data['column_count']):
                    name = lws[i + data['ind_name']].split(',')
                    if len(name) == 2:
                        name_country = name[1].strip()
                    else:
                        name_country = name[2].strip()
                    name_city = name[0].strip()
                    city_id = common.get_city_id(name_city, cities)
                    if city_id is None:
                        country_id = common.get_country_id(name_country, countries)
                        if country_id:
                            params = dict()
                            params["country"] = country_id
                            params['sh_name'] = name_city
                            params['name_rus'] = GoogleTranslator(source='en', target='ru').translate(name_city)
                            # записать новый город
                            params = {"schema_name": config.SCHEMA, "object_code": "cities", "values": params}
                            ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params,
                                                                      token_user=self.token)
                            if not ok:
                                print(ans)
                            else:
                                need_reload = True
                if need_reload:
                    cities = common.load_cities(self.token)  # прочитать новый список
                for i in range(0, len(lws), data['column_count']):
                    name = lws[i + data['ind_name']].split(',')
                    name_city = name[0].strip()
                    value = lws[i + data['ind_value']]
                    city_id = common.get_city_id(name_city, cities)
                    if city_id and value and value != '-':
                        count_row += 1
                        st_sql = get_st_sql_cities(st_sql, date, city_id, value, data['param_name'])
                # записать исторические данные
                common.write_script_db(st_sql, token=self.token)
        return count_row, ''
