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


class Spain(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Spain, self).__init__(source, code_function)

    def work(self):
        super(Spain, self).work()
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
        index = 0
        if is_ok:
            answer = json.loads(answer)
            countries = common.load_from_db('countries', where="sh_name='Spain'")
            if countries is None:
                print('Отсутствует страна Испания')
                return False
            country_id = countries[0]['id']
            for data in answer:
                index += 1
                count_row = 0
                if data['param_name'] in ['pops', 'area'] and data['object_code'] == 'provincies':
                    # записать провинции и города, а также население и площадь провинций Испании
                    count_row = self.get_spain_provinces(data, country_id)
                if data['param_name'] in ['pops'] and data['object_code'] == 'cities':
                    # записать города, а также население городов Испании
                    count_row = self.get_spain_cities(data, country_id)
                if count_row and count_row != 0:
                    result += count_row
        return result != 0

    def get_spain_provinces(self, elem, country_id):
        """
        Определение провинций (площадь или население)
        :param elem:
        :param country_id:
        :return:
        """
        def get_name(name):
            return name.strip().split('(')[0]

        lws = self.load_html(elem['code'])
        if lws:
            lws = BeautifulSoup(lws, 'html.parser').find_all('table')[0]. \
                find_all('tbody')[0].find_all('tr')
        if lws is None:
            return None, None
        year = 2017
        lws = lws[1:-1]
        # обновить список регионов
        regions = common.load_from_db('continent')
        need_reload = False
        for data in lws:
            unit = data.find_all('td')
            region = unit[9].text.strip()
            if self.refresh_regions(region, regions):
                need_reload = True
        if need_reload:
            # прочитать обновленный список регионов
            regions = common.load_from_db('continent')

        # обновить список провинций
        provinces = common.load_provinces_db(country_id)
        need_reload = False
        for data in lws:
            unit = data.find_all('td')
            name = get_name(unit[2].text)
            region = unit[9].text.strip()
            region_id = common.get_region_id(region, regions, pr=False)
            if self.refresh_provinces(name, None, None, country_id, region_id, provinces):
                need_reload = True
        if need_reload:
            # прочитать обновленный список провинций
            provinces = common.load_provinces_db(country_id)
        # для провинций записать информацию по колонке
        st_query = ''
        count_row = 0
        for data in lws:
            unit = data.find_all('td')
            if elem['param_name'] == 'pops':
                value = unit[elem['ind_value']].text.strip().replace(' ', '').replace(chr(160), '').replace('.', '')
            else:
                st = unit[elem['ind_value']].find().text.split('&')
                value = ''
                for s in st:
                    value += s
            name = get_name(unit[2].text)
            province_id = common.get_province_id(name, provinces, None, pr=False)
            if value:
                count_row += 1
                st_query += "select {schema}.pw_his_provinces('{param_name}', '{date}', {province_id}, {value});\n".\
                    format(date=str(year) + '-01-01', province_id=province_id, value=value, schema=config.SCHEMA,
                           param_name=elem['param_name'])
        common.write_script_db(st_query, self.token)
        return count_row

    def refresh_regions(self, name, regions):
        need_reload = False
        region_id = common.get_region_id(name, regions, pr=False)
        if region_id is None:
            values = dict()
            values['sh_name'] = name
            values['code'] = GoogleTranslator(source='ru', target='en').translate(name)
            # записать новый регион
            params = {"schema_name": config.SCHEMA, "object_code": "continent", "values": values}
            ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=self.token)
            if not ok:
                print(ans)
            else:
                need_reload = True
        return need_reload

    def refresh_provinces(self, name, code, status, country_id, region_id, provinces):
        need_reload = False
        province_id = common.get_province_id(name, provinces, code=None, pr=False)
        if province_id is None:
            values = dict()
            values["country"] = country_id
            values['sh_name'] = GoogleTranslator(source='ru', target='en').translate(name)
            values['name_own'] = GoogleTranslator(source='ru', target='es').translate(name)
            values['name_rus'] = name
            values['country'] = country_id
            if region_id:
                values['region'] = region_id
            if code:
                values['code'] = code
            if status:
                values['status'] = status
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

    def get_spain_cities(self, elem, country_id):
        """
        Определение провинций (площадь или население)
        :param elem:
        :param country_id:
        :return:
        """
        def get_name(name):
            return name.strip().split('(')[0]

        lws = self.load_html(elem['code'])
        if lws:
            lws = BeautifulSoup(lws, 'html.parser').find_all('table')[0]. \
                find_all('tbody')[0].find_all('tr')
        if lws is None:
            return None, None
        year = 2024
        # lws = lws[1:]

        # обновить список городов
        cities = common.load_from_db('cities', 'country={country}'.format(country=country_id))
        need_reload = False
        for data in lws:
            unit = data.find_all('td')
            name = get_name(unit[0].text)
            if self.refresh_cities(name, country_id, None, cities):
                need_reload = True

        if need_reload:
            # прочитать обновленный список городов
            cities = common.load_from_db('cities', 'country={country}'.format(country=country_id))
        # для городов записать информацию по колонке
        st_query = ''
        st_city = ''
        count_row = 0
        for data in lws:
            unit = data.find_all('td')
            value = unit[1].text.replace(chr(194), '').replace(chr(160), '').replace(' ', '')
            name = get_name(unit[0].text)
            city_id = common.get_city_id(name, cities, pr=False)
            if value:
                count_row += 1
                st_query += "select {schema}.pw_his_cities('{param_name}', '{date}', {city_id}, {value});\n".\
                    format(date=str(year) + '-01-01', city_id=city_id, value=value, schema=config.SCHEMA,
                           param_name=elem['param_name'])
                st_city += "update {schema}.nsi_cities set population={value} where id={city_id};select 1;\n". \
                    format(city_id=city_id, value=value, schema=config.SCHEMA)
        common.write_script_db(st_query, self.token)
        common.write_script_db(st_city, self.token)
        return count_row


# Spain('Испания', 'spain').start()
# while True:
#     time.sleep(5)
#
