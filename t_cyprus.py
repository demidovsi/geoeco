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


class Cyprus(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Cyprus, self).__init__(source, code_function)

    def work(self):
        super(Cyprus, self).work()
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
        url = "v1/select/{schema}/nsi_import?where=sh_name='{code_function}' and active".format(
            schema=config.SCHEMA, code_function=self.code_parser)
        answer, is_ok, status = common.send_rest(url)
        result = 0
        if is_ok:
            answer = json.loads(answer)
            countries = common.load_from_db('countries', where="sh_name='Cyprus'")
            if countries is None:
                print('Отсутствует страна Кипр')
                return False
            country_id = countries[0]['id']
            for data in answer:
                if data['param_name'] in ['pops'] and data['object_code'] == 'provincies':  # записать провинции Кипра
                    if self.get_cyprus_provinces(data, country_id):
                        result += 1
        return result != 0

    def get_cyprus_provinces(self, elem, country_id):
        """
        Определение провинций и городов провинций
        :param elem:
        :param country_id:
        :return:
        """
        lws = self.load_html(elem['code'])
        if lws:
            lws = BeautifulSoup(lws, 'html.parser').find_all('table')[0]. \
                find_all('tbody')[0].find_all('tr')
        if lws is None:
            return False
        lws_0 = lws[0]
        lws = lws[1:]
        # обновить список провинций
        need_load = False
        provinces = common.load_provinces_db(country_id)
        for data in lws:
            unit = data.find_all('td')
            name = get_name(unit[7].text)
            if self.refresh_provinces(name, country_id, provinces):
                need_load = True
        if need_load:
            provinces = common.load_provinces_db(country_id)

        # обновить список городов
        cities = common.load_from_db('cities', 'country={country}'.format(country=country_id))
        need_load = False
        for data in lws:
            unit = data.find_all('td')
            name = get_name(unit[7].text)
            name_city = get_name(unit[1].text)
            province_id = common.get_province_id(name, provinces, None, pr=False)
            if self.refresh_cities(name_city, country_id, province_id, cities):
                need_load = True
        if need_load:
            cities = common.load_from_db('cities', 'country={country}'.format(country=country_id))

        # записать исторические данные
        return self.get_cyprus_cities(lws, lws_0, elem, cities)

    def refresh_provinces(self, name, country_id, provinces):
        need_reload = False
        province_id = common.get_province_id(name, provinces, code=None, pr=False)
        if province_id is None:
            values = dict()
            values["country"] = country_id
            values['sh_name'] = GoogleTranslator(source='ru', target='en').translate(name)
            values['name_own'] = GoogleTranslator(source='ru', target='el').translate(name)
            values['name_rus'] = name
            values['country'] = country_id
            # записать новую провинцию страны
            params = {"schema_name": config.SCHEMA, "object_code": "provinces", "values": values}
            ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=self.token)
            if not ok:
                print(ans)
            else:
                provinces.append(values)
                ans, ok, status_result = common.send_rest(
                    "v1/select/{schema}/nsi_provinces?where=country={country_id} and name_rus='{name}'".format(
                        schema=config.SCHEMA, name=name, country_id=country_id))
                if ok:
                    values['id'] = json.loads(ans)[0]['id']
                need_reload = True
        return need_reload

    def refresh_cities(self, name, country_id, province_id, cities):
        need_reload = False
        city_id = common.get_city_id(name, cities, pr=False)
        if city_id is None:
            values = dict()
            values["country"] = country_id
            if province_id:
                values["province"] = province_id
            values['name_rus'] = GoogleTranslator(source='es', target='ru').translate(name)
            values['sh_name'] = GoogleTranslator(source='es', target='en').translate(name)
            values['name_own'] = name
            # записать новый город
            params = {"schema_name": config.SCHEMA, "object_code": "cities", "values": values}
            ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=self.token)
            if not ok:
                print(ans)
            else:
                need_reload = True
        return need_reload

    def get_cyprus_cities(self, lws, lws_0, elem, cities):
        """
        Исторические данные по населению городов Кипра
        :param elem:
        :param country_id:
        :return:
        """
        unit = lws_0.find_all('th')
        year1 = unit[4].text.split('(')[1].split(' ')[0]
        year2 = unit[5].text.split('(')[1].split(' ')[0]

        # для городов записать информацию по колонке
        st_query = ''
        st_city = ''
        for data in lws:
            unit = data.find_all('td')
            value1 = unit[4].text.replace(chr(194), '').replace(chr(160), '').replace(' ', '').strip()
            value2 = unit[5].text.replace(chr(194), '').replace(chr(160), '').replace(' ', '').strip().replace(chr(8599), '')
            name = get_name(unit[1].text)
            city_id = common.get_city_id(name, cities, pr=False)
            if value1:
                st_query += "select {schema}.pw_his_cities('{param_name}', '{date}', {city_id}, {value});\n".\
                    format(date=str(year1) + '-01-01', city_id=city_id, value=value1, schema=config.SCHEMA,
                           param_name=elem['param_name'])
            if value2:
                st_query += "select {schema}.pw_his_cities('{param_name}', '{date}', {city_id}, {value});\n".\
                    format(date=str(year2) + '-01-01', city_id=city_id, value=value2, schema=config.SCHEMA,
                           param_name=elem['param_name'])
                st_city += "update {schema}.nsi_cities set population={value} where id={city_id};select 1;\n". \
                    format(city_id=city_id, value=value2, schema=config.SCHEMA)
        result = common.write_script_db(st_query, self.token)
        result1 = common.write_script_db(st_city, self.token)
        return result and result1
