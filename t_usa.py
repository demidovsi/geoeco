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


def get_name(name):
    return name.strip().split('(')[0].split('[')[0].strip()


def get_date(text):
    return '20' + get_name(text).split(' 20')[1].split('г')[0].strip() + '-01-01'


class Usa(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Usa, self).__init__(source, code_function)

    def work(self):
        super(Usa, self).work()
        return self.load_list_indicators()

    def load_html(self, url):
        t = time.time()
        session = requests.Session()
        session.mount(url, http_adapter)
        r = session.get(url, timeout=(100, 100))
        try:
            if r.ok:
                return r.text
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
        t0 = time.time()
        answer = self.load_indicators(True)
        if answer:
            countries = common.load_from_db('countries', where="sh_name='United States'")
            if countries is None:
                print('Отсутствуют США')
                return False
            country_id = countries[0]['id']
            for data in answer:
                if data['object_code'] == 'provincies':
                    count_row = self.import_provinces(data, country_id)
                    common.write_log_db(
                        'import', self.source, 'Штаты США', page=count_row, td=time.time() - t0,
                        file_name=common.get_computer_name() + '\n поток="' + self.code_parser +
                        '"; param_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                        token_admin=self.token)
                    if count_row and count_row != 0:
                        result += count_row
        return result != 0

    def check_import_metric(self):
        super(Usa, self).check_import_metric()
        if len(self.list_start_metric) == 0:
            return
        countries = common.load_from_db('countries', where="sh_name='United States'")
        if countries is None:
            return
        country_id = countries[0]['id']
        answer = self.load_indicators(False)
        if answer:
            for indicator in self.list_start_metric:
                t0 = time.time()
                for data in answer:
                    if data['param_name'] == indicator:
                        if data['object_code'] == 'provincies':
                            count_row = self.import_provinces(data, country_id)
                            self.decode_finish_one_indicator(data, count_row, '', t0)

    def import_provinces(self, elem, country_id):
        """
        Определение провинций и городов провинций со столицами
        :param elem:
        :param countries:
        :return:
        """
        lws = self.load_html(elem['code'])
        if lws:
            lws = BeautifulSoup(lws, 'html.parser').find_all('table')[0]. \
                find_all('tbody')[0].find_all('tr')
        if lws is None:
            return False
        lws_0 = lws[0]
        year1 = get_date(lws_0.find_all('th')[6].text)
        year2 = get_date(lws_0.find_all('th')[7].text)
        year_area = get_date(lws_0.find_all('th')[11].text)
        lws = lws[1:]
        # обновить список провинций
        need_load = False
        provinces = common.load_provinces_db(country_id)
        st_query = ''
        for data in lws:
            unit = data.find_all('td')
            if unit[0].text.strip().isdigit():
                up, st = self.refresh_provinces(unit, country_id, provinces, year1, year2, year_area)
                if up:
                    need_load = True
                st_query += st
        common.write_script_db(st_query, self.token)
        if need_load:
            provinces = common.load_provinces_db(country_id)

        # обновить список городов
        cities = common.load_from_db('cities', 'country={country}'.format(country=country_id))
        st_query = ''
        for data in lws:
            unit = data.find_all('td')
            if unit[0].text.strip().isdigit():
                name = get_name(unit[2].text)
                province_id = common.get_province_id(name, provinces, None, pr=False)
                name_city = get_name(unit[15].text)
                self.refresh_cities(name_city, country_id, province_id, cities)
                name_city = get_name(unit[14].text)
                up, st = self.refresh_cities(name_city, country_id, province_id, cities, capital=True)
                st_query += st
        common.write_script_db(st_query, self.token)
        return len(provinces)

    def refresh_cities(self, name, country_id, province_id, cities, capital=False):
        need_reload = False
        city_id = common.get_city_id(name, cities, pr=False)
        st_nsi = ''
        if city_id is None:
            values = dict()
            values["country"] = country_id
            if province_id:
                values["province"] = province_id
            values['name_rus'] = name
            values['sh_name'] = GoogleTranslator(source='ru', target='en').translate(name)
            values['name_own'] = values['sh_name']
            # записать новый город
            params = {"schema_name": config.SCHEMA, "object_code": "cities", "values": values}
            ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=self.token)
            if not ok:
                print(ans)
                return False
            else:
                cities.append(values)
                ans, ok, status_result = common.send_rest(
                    "v1/select/{schema}/nsi_cities?where=country={country_id} and name_rus='{name}'".format(
                        schema=config.SCHEMA, name=name, country_id=country_id))
                if ok:
                    city_id = json.loads(ans)[0]['id']
                    values['id'] = city_id

                need_reload = True
        else:
            st_nsi += "update {schema}.nsi_cities set province={province_id} where id={id};select 1;\n". \
                format(province_id=province_id, id=city_id, schema=config.SCHEMA)
        if capital:
            st_nsi += "update {schema}.nsi_provinces set capital={city_id} where id={id};select 1;\n".\
                format(id=province_id, city_id=city_id, schema=config.SCHEMA)
        # common.write_script_db(st_nsi, self.token)
        return need_reload, st_nsi

    def refresh_provinces(self, unit, country_id, provinces, year1, year2, year_area):
        need_reload = False
        name = get_name(unit[2].text)
        sh_name = get_name(unit[3].text)
        code = get_name(unit[4].text)
        pop1 = get_name(unit[6].text)
        pop2 = get_name(unit[7].text)
        area = get_name(unit[11].text)
        try:
            flag_svg = unit[1].find_all('img')[0]['src']
        except:
            flag_svg = ''
        province_id = common.get_province_id(name, provinces, code=None, pr=False)
        if province_id is None:
            values = dict()
            values["country"] = country_id
            values['sh_name'] = sh_name
            values['name_own'] = GoogleTranslator(source='ru', target='es').translate(name)
            values['name_rus'] = name
            values['country'] = country_id
            if flag_svg:
                values['flag_svg'] = flag_svg
            if code:
                values['code'] = code
            # записать новую провинцию страны
            params = {"schema_name": config.SCHEMA, "object_code": "provinces", "values": values}
            ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=self.token)
            if not ok:
                print(ans)
                return False
            else:
                provinces.append(values)
                ans, ok, status_result = common.send_rest(
                    "v1/select/{schema}/nsi_provinces?where=country={country_id} and name_rus='{name}'".format(
                        schema=config.SCHEMA, name=name, country_id=country_id))
                if ok:
                    province_id = json.loads(ans)[0]['id']
                    values['id'] = province_id
                need_reload = True
        st_query = ''
        st_query += "select {schema}.pw_his_provinces('{param_name}', '{date}', {id}, {value});\n". \
            format(date=str(year1), id=province_id, value=pop1, schema=config.SCHEMA, param_name='pops')
        st_query += "select {schema}.pw_his_provinces('{param_name}', '{date}', {id}, {value});\n". \
            format(date=str(year2), id=province_id, value=pop2, schema=config.SCHEMA, param_name='pops')
        st_query += "select {schema}.pw_his_provinces('{param_name}', '{date}', {id}, {value});\n". \
            format(date=str(year_area), id=province_id, value=area, schema=config.SCHEMA, param_name='area')

        st_nsi = "update {schema}.nsi_provinces set population={value} where id={id};\n". \
            format(id=province_id, value=pop2, schema=config.SCHEMA)
        st_nsi += "update {schema}.nsi_provinces set square={value} where id={id};select 1;\n". \
            format(id=province_id, value=area, schema=config.SCHEMA)
        # common.write_script_db(st_query, self.token)
        # common.write_script_db(st_nsi, self.token)
        return need_reload, st_query + st_nsi


Usa('США', 'usa').start()
while True:
    time.sleep(5)


