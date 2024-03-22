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


class Georgia(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Georgia, self).__init__(source, code_function)

    def work(self):
        super(Georgia, self).work()
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
            countries = common.load_from_db('countries', where="sh_name='Georgia'")
            if countries is None:
                print('Отсутствует страна Грузия')
                return False
            country_id = countries[0]['id']
            for data in answer:
                index += 1
                count_row = 0
                if data['param_name'] in ['pops'] and data['object_code'] == 'cities':
                    # записать провинции и города, а также население городов Грузии
                    count_row = self.get_provinces(data, country_id)
                if count_row and count_row != 0:
                    result += count_row
        return result != 0

    def get_provinces(self, elem, country_id):
        """
        Определение провинций (площадь или население)
        :param elem:
        :param country_id:
        :return:
        """
        def get_name(name):
            name = name.strip().split('(')[0]
            return name.strip().split('[')[0]

        lws = self.load_html(elem['code'])
        if lws:
            lws = BeautifulSoup(lws, 'html.parser').find_all('table')[0]. \
                find_all('tbody')[0].find_all('tr')
        if lws is None:
            return None, None
        years = [1989, 2014, 2019, 2020]
        num_columns = [3, 5, 6, 7]
        k = [1, 1, 1000, 1]
        lws = lws[1:]

        # обновить список провинций
        provinces = common.load_provinces_db(country_id)
        need_reload = False
        for data in lws:
            unit = data.find_all('td')
            name = get_name(unit[8].text)
            if name == 'Самцхе-Джавахети':
                name = 'Самцхе-Джавахетия'
            if self.refresh_provinces(name, country_id, provinces):
                need_reload = True
        if need_reload:
            # прочитать обновленный список провинций
            provinces = common.load_provinces_db(country_id)
        # обновить список городов
        cities = common.load_from_db('cities', where='country={country_id}'.format(country_id=country_id))
        need_reload = False
        for data in lws:
            unit = data.find_all('td')
            name = get_name(unit[1].text)
            name_own = get_name(unit[2].text)
            name_province = get_name(unit[8].text)
            if name_province == 'Самцхе-Джавахети':
                name_province = 'Самцхе-Джавахетия'
            province_id = common.get_province_id(name_province, provinces, None, pr=False)
            if self.refresh_cities(name, name_own, country_id, province_id, cities):
                need_reload = True
        # обновить список городов
        if need_reload:
            cities = common.load_from_db('cities', where='country={country_id}'.format(country_id=country_id))
        # для городов записать информацию по колонке
        st_query = ''
        st_city = ''
        count_row = 0
        for data in lws:
            unit = data.find_all('td')
            name = get_name(unit[1].text)  # имя города
            city_id = common.get_city_id(name, cities, pr=False)
            if elem['param_name'] == 'pops':
                for i, year in enumerate(years):
                    try:
                        value = float(unit[num_columns[i]].text.strip().replace(' ', '').replace(chr(160), '')
                                      .replace('.', '')) * k[i]
                        st_query += "select {schema}.pw_his_cities('{param_name}', '{date}', {city_id}, " \
                                    "{value});select 1;\n". \
                            format(date=str(years[i]) + '-01-01', city_id=city_id,
                                   value=value, schema=config.SCHEMA, param_name=elem['param_name'])
                    except:
                        pass
                try:
                    value = float(unit[num_columns[-1]].text.strip().replace(' ', '').replace(chr(160), '')
                                  .replace('.', '')) * k[-1]
                    st_city += "update {schema}.nsi_cities set population={value} where id={city_id};select 1;\n". \
                        format(city_id=city_id, value=value, schema=config.SCHEMA)
                    count_row += 1
                except:
                    pass
        common.write_script_db(st_query, self.token)
        common.write_script_db(st_city, self.token)
        return count_row

    def refresh_provinces(self, name, country_id, provinces):
        need_reload = False
        province_id = common.get_province_id(name, provinces, code=None, pr=False)
        if province_id is None:
            values = dict()
            values["country"] = country_id
            values['sh_name'] = GoogleTranslator(source='ru', target='en').translate(name)
            values['name_own'] = GoogleTranslator(source='ru', target='ka').translate(name)
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

    def refresh_cities(self, name, name_own, country_id, province_id, cities):
        need_reload = False
        city_id = common.get_city_id(name, cities, pr=False)
        if city_id is None:
            values = dict()
            values["country"] = country_id
            values["province"] = province_id
            values['sh_name'] = GoogleTranslator(source='ru', target='en').translate(name)
            values['name_rus'] = name
            values['name_own'] = name_own
            # записать новый город
            params = {"schema_name": config.SCHEMA, "object_code": "cities", "values": values}
            ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=self.token)
            if not ok:
                print(ans)
            else:
                need_reload = True
        return need_reload
