import config
import trafaret_thread
import common
import countries as c_countries
import time
import json
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

http_adapter = HTTPAdapter(max_retries=10)
obj = None
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/114.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Cookie": "rbzid=oo6XcliuU2QC8We87874BO9GA9Pg41SRibMzr0fR4Pso9n1VAQKm0oyrnLJPVriQf9JI2FTIAoCtAV4KnJPeVGRS4r"
              "FQgN0ATx7FeG5ZsPhPER2d4NunXqSNLzg7eF+v3AlmD7G/hNyUlA5matUZg84CJQfK7RPsmDJVvhF+lDGlaBU8veOPv/ir/"
              "BtL3k6x503nnMNKWVQHCTrqwrUCWVQPIH7wmGKCucKl8DO4PwVIpJ4CmxNG0jx3/yDiSQUW; "
              "rbzsessionid=887378f03050c7c41a47db6ab65b13ff; _gid=GA1.3.1186344673.1686827652; "
              "WSS_FullScreenMode=false; deviceChannel=Default; _gat=1; _ga=GA1.1.312130872.1686827652; "
              "_ga_F5VXQ9Z7N5=GS1.1.1686827652.1.1.1686827968.0.0.0",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": "Not.A/Brand\";v=\"8\", \"Chromium\";v=\"114\", \"Google Chrome\";v=\"114\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"Windows\"",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1"
}


class Countries(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Countries, self).__init__(source, code_function)

    def load_html_merkator(self, url, table=0, with_header=True):
        t = time.time()
        try:
            session = requests.Session()
            session.mount(url, http_adapter)
            if with_header:
                r = session.get(url, headers=headers, timeout=(100, 100))
            else:
                r = session.get(url, timeout=(100, 100))
            if r.ok:
                lws = BeautifulSoup(r.text, 'html.parser').find_all('table')[table].find_all('tbody')[0].find_all('tr')
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

    def make_merkator(self, lws, countries, param_name, t1, caption):
        values = list()
        st_absent = ''
        for data in lws:
            unit = data.find_all('td')
            name_country = unit[1].text.split('[')[0]
            name_type = unit[2].text
            country_id = common.get_country_id(name_country, countries, None, False)
            if country_id is None:
                st_absent = st_absent + ', ' if st_absent else st_absent
                st_absent += name_country
            else:
                params = dict()
                params['id'] = country_id
                params[param_name] = name_type
                values.append(params)
        if st_absent:
            caption += ';\n не найдены страны: "' + st_absent + '"'
        common.write_objects_db('countries', values, token=self.token)
        td = time.time() - t1
        common.write_log_db(
            'import', self.source, caption, page=len(values), td=td,
            file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '"; param_name=' + param_name,
            token_admin=self.token)
        return len(values) > 0

    def make_member_es(self, lws, countries, t1):
        st_absent = ''
        st_query = ''
        count = 0
        for data in lws:
            unit = data.find_all('td')
            name_country = unit[1].text.split('[')[0].strip()
            year = unit[2].text.strip()
            country_id = common.get_country_id(name_country, countries, None, False)
            if country_id is None:
                st_absent = st_absent + ', ' if st_absent else st_absent
                st_absent += name_country
            else:
                st_query = st_query + 'update {schema}.nsi_countries set es_member={year} where id={country_id}; select 1;\n'.\
                    format(schema=config.SCHEMA, year=year, country_id=country_id)
                count += 1
        result = common.write_script_db(st_query, token=self.token)
        caption = 'Принадлежность стран к Евросоюзу'
        td = time.time() - t1
        if result:
            if st_absent:
                caption += ';\n не найдены страны: "' + st_absent + '"'
            common.write_log_db(
                'import', self.source, caption, page=count, td=td,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '"; es_member',
                token_admin=self.token)
        return result

    def import_json(self):
        t1 = time.time()
        url = 'https://restcountries.com/v3.1/all'
        session = requests.Session()
        session.mount(url, http_adapter)
        r = session.get(url, timeout=(100, 100))
        if r.ok:
            return json.loads(r.text)
        else:
            td = time.time() - t1
            common.write_log_db(
                'Error', self.source, url + ': ' + str(r.status_code) + '= ' + str(r.text),
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '";',
                td=td, token_admin=self.token)

    def load_list_countries(self, answer):
        # формирование таблицы nsi_list_countries (список всех стран)
        t0 = time.time()
        list_countries = list()
        for data in answer:
            param = dict()
            if 'cca3' not in data or 'name' not in data:
                continue
            param['code'] = data['cca3']
            param['sh_name'] = data['name']['official']
            list_countries.append(param)
        url = 'v1/objects/{schema}/list_countries'.format(schema=config.SCHEMA)
        t1 = time.time()
        ans, is_ok, status_response = common.send_rest(url)  # прочитать существующие страны (для определения ID)
        td = time.time() - t1
        if not is_ok:
            common.write_log_db(
                'Error', self.source, 'Чтение списка стран; ' + str(ans), td=td,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '";',
                token_admin=self.token)
            return False
        ans = json.loads(ans)['values']
        for data in answer:
            for unit in list_countries:
                if unit['code'] == data['code']:
                    unit['id'] = data['id']  # эта страна будет корректироваться (на всякий случай)
                    break
        result = common.write_objects_db('list_countries', list_countries, token=self.token) is None
        td = time.time() - t0
        if result:
            common.write_log_db(
                'import', self.source, 'Списки стран', page=len(list_countries), td=td,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '"; nsi_list_countries',
                token_admin=self.token)
        return result

    def load_tld(self, answer):
        # формирование таблицы nsi_tld (суффиксы интернета для всех стран)
        t0 = time.time()
        tld = list()
        for data in answer:
            if 'tld' not in data:
                continue
            cur = data['tld']
            for key in cur:
                if key not in tld:
                    tld.append({"sh_name": key})
        url = 'v1/objects/{schema}/tld'.format(schema=config.SCHEMA)
        t1 = time.time()
        ans, is_ok, status_response = common.send_rest(url)  # прочитать существующие суффиксы (для определения ID)
        td = time.time() - t1
        if not is_ok:
            # print(str(answer))
            common.write_log_db(
                'Error', self.source, 'Чтение суффиксов стран в интернете; ' + str(ans), td=td,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '";',
                token_admin=self.token)
            return False
        ans = json.loads(ans)['values']
        for data in ans:
            for unit in tld:
                if unit['sh_name'] == data['sh_name']:
                    unit['id'] = data['id']  # этот суффикс будет корректироваться (на всякий случай)
                    break
        # записать изменения или внести новые
        result = common.write_objects_db('tld', tld, token=self.token) is None
        td = time.time() - t0
        if result:
            common.write_log_db(
                'import', self.source, 'Суффиксы для интернета', page=len(tld), td=td,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '"; nsi_tld',
                token_admin=self.token)
        return result

    def load_languages(self, answer):
        # формирование таблицы nsi_languages (официальные языуи для всех стран)
        t0 = time.time()
        t1 = time.time()
        languages = list()
        for data in answer:
            if 'languages' not in data:
                continue
            cur = data['languages']
            for key in cur.keys():
                exist = False
                for unit in languages:
                    if unit['code'] == key:
                        exist = True
                        break
                if not exist:
                    languages.append({"code": key, "sh_name": cur[key]})
        url = 'v1/objects/{schema}/languages'.format(schema=config.SCHEMA)
        ans, is_ok, status_response = common.send_rest(url)  # прочитать существующие языки (для определения ID)
        td = time.time() - t1
        if not is_ok:
            common.write_log_db(
                'Error', self.source, 'Чтение языков стран; ' + str(ans), td=td,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '";',
                token_admin=self.token)
            return False
        ans = json.loads(ans)['values']
        for data in ans:
            for unit in languages:
                if unit['code'] == data['code']:
                    unit['id'] = data['id']  # эта валюта будет корректироваться (на всякий случай)
                    break
        # записать изменения или внести новые
        result = common.write_objects_db('languages', languages, token=self.token) is None
        td = time.time() - t0
        if result:
            common.write_log_db(
                'import', self.source, 'Официальные языки', page=len(languages), td=td,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '"; nsi_languages',
                token_admin=self.token)
        return result

    def load_currencies(self, answer):
        # формирование таблицы nsi_currencies (валюты для всех стран)
        t0 = time.time()
        currencies = list()
        for data in answer:
            if 'currencies' not in data:
                continue
            cur = data['currencies']
            for key in cur.keys():
                exist = False
                for unit in currencies:
                    if unit['code'] == key:
                        exist = True
                        break
                if not exist:
                    currencies.append({"code": key, "sh_name": cur[key]['name'],
                                       "symbol": cur[key]['symbol'] if 'symbol' in cur[key] else ''})
        url = 'v1/objects/{schema}/currencies'.format(schema=config.SCHEMA)
        t1 = time.time()
        ans, is_ok, status_response = common.send_rest(url)  # прочитать существующие валюты (для определения ID)
        td = time.time() - t1
        if not is_ok:
            common.write_log_db(
                'Error', self.source, 'Чтение списка валют ' + str(ans), td=td,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '";',
                token_admin=self.token)
            return False
        ans = json.loads(ans)['values']
        for data in ans:
            for unit in currencies:
                if unit['code'] == data['code']:
                    unit['id'] = data['id']  # эта валюта будет корректироваться (на всякий случай)
                    break
        result = common.write_objects_db('currencies', currencies) is None
        if result:
            t1 = time.time()
            url = 'v1/objects/{schema}/currencies'.format(schema=config.SCHEMA)
            ans, is_ok, status_response = common.send_rest(url)  # прочитать валюты с ID
            td = time.time() - t1
            if not is_ok:
                common.write_log_db(
                    'Error', self.source, 'Чтение списка валют из БД ' + str(ans), td=td,
                    file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '";',
                    token_admin=self.token)
                return False
            ans = json.loads(ans)['values']
            params = list()
            for data in ans:
                params.append({'schema_name': config.SCHEMA, "object_code": "currencies", "param_code": "course",
                               "obj_id": data['id'], 'discret_sec': 86400, "type_his": "data"})
            t1 = time.time()
            ans, is_ok, status_response = common.send_rest(
                'v1/MDM/his/link', directive='PUT', params=params, token_user=self.token)
            td = time.time() - t0
            if is_ok:
                common.write_log_db(
                    'import', self.source, 'Список валют', td=td, page=len(params),
                    file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '"; nsi_currencies',
                    token_admin=self.token)
                return True
            else:
                common.write_log_db(
                    'Error', self.source, 'Запись параметров истории валют  ' + str(ans), td=td,
                    file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '"; nsi_currencies',
                    token_admin=self.token)
                return False
        else:
            return False

    def work(self):
        super(Countries, self).work()
        countries = c_countries.load_countries(token=self.token)
        if countries is None:
            print('Нет стран')
            return False

        # прочитать json для всех стран
        answer = self.import_json()
        if answer is None:
            return False
        result = self.load_tld(answer)
        if result:
            result = self.load_languages(answer)
        if result:
            result = self.load_currencies(answer)
        if result:
            result = self.load_list_countries(answer)

        if result:
            t1 = time.time()
            lws = self.load_html_merkator(
                'https://ru.wikipedia.org/wiki/%D0%95%D0%B2%D1%80%D0%BE%D0%BF%D0%B5%D0%B9%D1%81%D0%BA%D0%B8%D0%B9_'
                '%D1%81%D0%BE%D1%8E%D0%B7', table=3, with_header=False)
            if lws:
                result = self.make_member_es(lws[1:], countries, t1)

        if result:
            t1 = time.time()
            lws = self.load_html_merkator('https://merkator.org.ua/ru/spravochnik/formy-pravleniya-stran-mira/')
            if lws:
                result = self.make_merkator(lws, countries, 'government', t1, 'Форма организации государства')
        if result:
            t1 = time.time()
            lws = self.load_html_merkator(
                'https://merkator.org.ua/ru/spravochnik/formy-gosudarstvennogo-ustroystva-stran-mira/')
            if lws:
                result = self.make_merkator(lws, countries, 'type_government', t1, 'Тип государственного устройства')
        if result:
            t1 = time.time()

        return result
