import trafaret_thread
import common
import time
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from deep_translator import GoogleTranslator

http_adapter = HTTPAdapter(max_retries=10)
obj = None


class StatData(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(StatData, self).__init__(source, code_function)

    def work(self):
        super(StatData, self).work()
        return self.load_list_cities()

    def load_html(self, url):
        t = time.time()
        try:
            session = requests.Session()
            session.mount(url, http_adapter)
            r = session.get(url)
            if r.ok:
                lws = BeautifulSoup(r.text, 'html.parser').find_all('table')[0].find_all('tbody')[0].find_all('tr')[1:]
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

    def load_list_cities(self):
        t0 = time.time()
        lws = self.load_html("https://www.statdata.ru/largestcities_world")
        if lws is None:
            return False
        countries = common.load_countries(self.token)
        if countries is None:
            print('Отсутствуют страны')
            return False
        cities = common.load_cities(self.token)
        values = list()
        # сформируем или скорректируем список крупнейших городов мира
        st_absent = ''
        for i, data in enumerate(lws):
            # data = data.replace('</td>', ';').replace('<td>', '').replace('</tr>', '').replace('N / A', '')
            data = data.find_all('td')
            lws[i] = data
            # unit = data.strip().split(';')
            name_city = data[1].text
            name_country = data[2].text
            population_2018 = data[3].text.replace(' ', '').replace('N/A', '')
            population = None
            area = None
            if len(data) >= 5:
                population = data[4].text.replace(' ', '').replace('N/A', '')
            if len(data) >= 6:
                area = data[5].text.replace(' ', '').replace('N/A', '')
            # найти id страны
            country_id = common.get_country_id(name_country, countries)
            if country_id:
                params = dict()
                params["name_rus"] = name_city
                params["country"] = country_id
                params['population'] = population if population else population_2018
                params['sh_name'] = GoogleTranslator(source='ru', target='en').translate(name_city)
                if area:
                    params['square'] = area
                city_id = common.get_city_id(name_city, cities)
                if city_id:
                    params['id'] = city_id
                values.append(params)
            else:
                if name_country not in st_absent:
                    st_absent = st_absent + ', ' if st_absent else st_absent
                    st_absent += name_country
                print(name_country)

        common.write_objects_db('cities', values, self.token)
        # прочитать новый список городов
        cities = common.load_cities(self.token)
        st_sql = ''
        date = str(time.gmtime().tm_year) + '-' + str(time.gmtime().tm_mon).rjust(2, '0') + '-01'

        count_row = 0
        for data in lws:
            name_city = data[1].text
            population_2018 = data[3].text.replace(' ', '').replace('N/A', '')
            population = None
            area = None
            if len(data) >= 5:
                population = data[4].text.replace(' ', '').replace('N/A', '')
            if len(data) >= 6:
                area = data[5].text.replace(' ', '').replace('N/A', '')
            city_id = common.get_city_id(name_city, cities)
            if city_id:
                if population:
                    st_sql = common.get_st_sql(st_sql, 'pw_his_cities', date, city_id, population, 'pops')
                elif population_2018:
                    st_sql = common.get_st_sql(st_sql, 'pw_his_cities', '2018-12-01', city_id, population_2018, 'pops')
                if area:
                    st_sql = common.get_st_sql(st_sql, 'pw_his_cities', date, city_id, area, 'area')
                count_row += 1
        common.write_script_db(st_sql, self.token)

        td = time.time() - t0
        if count_row:
            st = 'Крупные города'
            if st_absent:
                st = ';\n не найдены страны: "' + st_absent + '"'
            common.write_log_db(
                'import', self.source, st, page=count_row, td=td,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + "; param_name=pops, area",
                token_admin=self.token)
            return True
        else:
            common.write_log_db(
                'Error', self.source, 'Крупные города; Не обнаружено данных', td=td,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + "; param_name=pops, area",
                token_admin=self.token)
            return False
