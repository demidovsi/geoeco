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


class Indonesia(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Indonesia, self).__init__(source, code_function)

    def work(self):
        super(Indonesia, self).work()
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
            countries = common.load_from_db('countries', where="sh_name='Indonesia'")
            if countries is None:
                print('Отсутствует страна Индонезия')
                return False
            country_id = countries[0]['id']
            for data in answer:
                index += 1
                count_row = 0
                if data['param_name'] in ['pops', 'area'] and data['object_code'] == 'provincies':
                    # записать провинции и города, а также население и площадь провинций Индонезии
                    count_row = self.get_indonesia_provinces(data, country_id)
                if data['param_name'] in ['pops'] and data['object_code'] == 'cities':
                    # записать провинции и города, а также население городов Индонезии
                    count_row = self.get_indonesia_pops_cities(data, country_id)
                if count_row and count_row != 0:
                    result += count_row
        return result != 0

    def get_indonesia_pops_cities(self, elem, country_id):
        """
        Определение провинций (площадь или население) и городов
        :param elem:
        :param country_id:
        :return:
        """
        lws = self.load_html(elem['code'])
        if lws:
            lws = BeautifulSoup(lws, 'html.parser').find_all('table', class_="wikitable sortable")[0]. \
                find_all('tbody')[0].find_all('tr')
        if lws is None:
            return None, None
        unit = lws[0].find_all('th')
        try:
            year1 = unit[4].text.split('(')[1].split(' ')[0]
        except:
            year1 = '2010'
        try:
            year2 = unit[5].text.split('(')[1].split(' ')[0]
        except:
            year2 = '2020'
        lws = lws[1:]
        # обновить список провинций
        provinces = common.load_provinces_db(country_id)
        need_reload = False
        for data in lws:
            unit = data.find_all('td')
            name = unit[3].text.replace('Особый округ', '').replace('Особый столичный округ', 'Джакарта').\
                replace('Острова', '').strip()
            if self.refresh_provinces(name, None, None, country_id, None, provinces):
                need_reload = True
        if need_reload:
            # прочитать обновленный список провинций
            provinces = common.load_provinces_db(country_id)
        # обновить список городов
        cities = common.load_from_db('cities', where='country={country_id}'.format(country_id=country_id))
        need_reload = False
        for data in lws:
            unit = data.find_all('td')
            city = unit[1].text.strip()
            name = unit[3].text.replace('Особый округ', '').replace('Особый столичный округ', 'Джакарта').\
                replace('Острова', '').strip()
            province_id = common.get_province_id(name, provinces, None, pr=False)
            if self.refresh_cities(city, country_id, province_id, cities):
                need_reload = True
        if need_reload:
            # прочитать обновленный список городов
            cities = common.load_from_db('cities', where='country={country_id}'.format(country_id=country_id))
        # для провинций записать информацию по колонкам
        st_query = ''
        count_row = 0
        values = list()
        for data in lws:
            unit = data.find_all('td')
            value1 = unit[4].text.replace(' ', '').replace(chr(160), '').strip()
            value2 = unit[5].text.replace(' ', '').replace(chr(160), '').strip()
            city = unit[1].text
            city_id = common.get_city_id(city, cities, pr=False)
            if value1:
                count_row += 1
                st_query += "select {schema}.pw_his_cities('{param_name}', '{date}', {city_id}, {value});\n".\
                    format(date=year1 + '-01-01', city_id=city_id, value=value1, schema=config.SCHEMA,
                           param_name=elem['param_name'])
            if value2:
                count_row += 1
                st_query += "select {schema}.pw_his_cities('{param_name}', '{date}', {city_id}, {value});\n".\
                    format(date=year2 + '-01-01', city_id=city_id, value=value2, schema=config.SCHEMA,
                           param_name=elem['param_name'])
                if elem['param_name'] == 'pops':
                    values.append({'id': city_id, 'population': value2})
        common.write_script_db(st_query, self.token)
        params = {"schema_name": config.SCHEMA, "object_code": "cities", "values": values}
        ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=self.token)
        if not ok:
            print(ans)
        return count_row

    def get_indonesia_provinces(self, elem, country_id):
        """
        Определение провинций (площадь или население) и городов
        :param elem:
        :param country_id:
        :return:
        """
        lws = self.load_html(elem['code'])
        if lws:
            lws = BeautifulSoup(lws, 'html.parser').find_all('table', class_="wikitable")[1]. \
                find_all('tbody')[0].find_all('tr')
        if lws is None:
            return None, None
        unit = lws[0].find_all('th')
        try:
            year = unit[6].text.split('(')[1].split(')')[0]
        except:
            year = '2020'
        lws = lws[1:-1]
        # обновить список регионов
        regions = common.load_from_db('continent')
        need_reload = False
        for data in lws:
            unit = data.find_all('td')
            region = unit[5].text
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
            name = unit[1].text
            code = unit[2].text
            status = unit[4].text
            region = unit[5].text
            region_id = common.get_region_id(region, regions, pr=False)
            if self.refresh_provinces(name, code, status, country_id, region_id, provinces):
                need_reload = True
        if need_reload:
            # прочитать обновленный список провинций
            provinces = common.load_provinces_db(country_id)
        # обновить список городов
        cities = common.load_from_db('cities', where='country={country_id}'.format(country_id=country_id))
        for data in lws:
            unit = data.find_all('td')
            name = unit[1].text
            city = unit[3].text
            province_id = common.get_province_id(name, provinces, None, pr=False)
            self.refresh_cities(city, country_id, province_id, cities)
        # для провинций записать информацию по колонке
        st_query = ''
        count_row = 0
        for data in lws:
            unit = data.find_all('td')
            value = unit[elem['ind_value']].text.replace(' ', '').replace(chr(160), '')
            name = unit[1].text
            province_id = common.get_province_id(name, provinces, None, pr=False)
            if value:
                count_row += 1
                st_query += "select {schema}.pw_his_provinces('{param_name}', '{date}', {province_id}, {value});\n".\
                    format(date=year + '-01-01', province_id=province_id, value=value, schema=config.SCHEMA,
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
            values['name_own'] = GoogleTranslator(source='ru', target='id').translate(name)
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
            values["province"] = province_id
            values['sh_name'] = GoogleTranslator(source='ru', target='en').translate(name)
            values['name_rus'] = name
            # записать новый город
            params = {"schema_name": config.SCHEMA, "object_code": "cities", "values": values}
            ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=self.token)
            if not ok:
                print(ans)
            else:
                need_reload = True
        return need_reload
