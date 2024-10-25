import trafaret_thread
import common
import config
import time
import requests
from bs4 import BeautifulSoup
from deep_translator import GoogleTranslator
from requests.adapters import HTTPAdapter

http_adapter = HTTPAdapter(max_retries=10)
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


class TradingEconomics(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(TradingEconomics, self).__init__(source, code_function)

    def work(self):
        super(TradingEconomics, self).work()
        return self.load_list_indicators()

    def load_html(self, indicator):
        t = time.time()
        # url = config.url_trading + indicator
        url = self.par['compliment_txt']['url'].format(indicator=indicator) + '?continent=world'
        session = requests.Session()
        session.mount(url, http_adapter)
        r = session.get(url, headers=headers, timeout=(100, 100))
        try:
            if r.ok:
                lws = BeautifulSoup(r.text, 'html.parser').find_all('table')[0].find_all('tr')[1:]
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

    def import_data(self, lws, param_name, countries):
        st_query = ''
        st_absent = ''
        count_row = 0
        for data in lws:
            values = data.find_all('td')
            name_country = values[0].text.strip()
            value_prev = values[1].text
            value = values[2].text
            date = values[3].text
            date_prev = date.split('-')
            date_prev = str(int(date_prev[0]) - 1) + '-' + date_prev[1]
            country_id = common.get_country_id(name_country, countries, None, False)
            if country_id:
                count_row += 1
                st_query += "select {schema}.pw_his('{param_name}', '{date}', {country_id}, {value});\n". \
                    format(param_name=param_name, date=date + '-01', country_id=country_id,
                           value=value, schema=config.SCHEMA)
                st_query += "select {schema}.pw_his('{param_name}', '{date}', {country_id}, {value});\n". \
                    format(param_name=param_name, date=date_prev + '-01', country_id=country_id,
                           value=value_prev, schema=config.SCHEMA)
            else:
                if name_country not in st_absent:
                    st_absent = st_absent + ', ' if st_absent else st_absent
                    st_absent = st_absent + name_country
        common.write_script_db(st_query, self.token)
        return count_row, st_absent

    def check_import_metric(self):
        super(TradingEconomics, self).check_import_metric()
        if len(self.list_start_metric) == 0:
            return
        countries = common.load_countries(self.token)
        if countries is None:
            return
        answer = self.load_indicators(False)
        if answer:
            for indicator in self.list_start_metric:
                t0 = time.time()
                for data in answer:
                    if data['object_code'] == 'countries' and data['param_name'] == indicator:
                        lws = self.load_html(data['code'])
                        if lws is None:
                            continue
                        count_row, st_absent = self.import_data(lws, data['param_name'], countries=countries)
                        self.decode_finish_one_indicator(data, count_row, st_absent, t0)

    def load_list_indicators(self):
        result = 0
        index = 0
        answer = self.load_indicators(True)
        if answer:
            countries = common.load_countries(token=self.token)
            if countries is None:
                print('Нет стран')
                return
            for data in answer:
                index += 1
                if data['object_code'] != 'countries':
                    continue
                t0 = time.time()
                lws = self.load_html(data['code'])
                if lws is None:
                    continue
                count_row, st_absent = self.import_data(lws, data['param_name'], countries=countries)
                if count_row and count_row != 0:
                    result += count_row
                    st = data['name_rus']
                    if st_absent:
                        st += ';\n не найдены страны: "' + st_absent + '"'
                    common.write_log_db(
                        'import', self.source, st, page=count_row, td=time.time() - t0,
                        law_id=str(index) + ' из ' + str(len(answer)),
                        file_name=common.get_computer_name() + '\n поток="' + self.code_parser +
                            '"; param_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                        token_admin=self.token)
        return result != 0


# TradingEconomics('trading', 'trading').start()
# while True:
#     time.sleep(5)
def import_indicators():
    def get_id(code):
        for unit in indicators:
            if unit['code'] == code:
                return unit['id']

    url = "https://ru.tradingeconomics.com/indicators"
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url, headers=headers, timeout=(100, 100))
    try:
        if r.ok:
            indicators = common.load_from_db('import', "sh_name='trading'")
            lws = BeautifulSoup(r.text, 'html.parser').find_all('li', class_='list-group-item')
            values = list()
            for data in lws:
                st = str(data.contents).split('>')
                code = st[0].split('href=')
                if len(code) < 2 or 'country-list' not in code[1]:
                    continue
                code = code[1].replace('"', '').replace("'", '').strip().split('/')[2].split(' ')[0]
                indicator_id = get_id(code)
                if indicator_id is None:
                    name_rus = st[1].split('<')[0]
                    name = GoogleTranslator(source='ru', target='en').translate(name_rus)
                    param_name = code.replace('-', '_')
                    param = {"sh_name": "trading", "code": code, "name": name, "object_code": "countries",
                             "name_rus": name_rus, "param_name": param_name}
                    values.append(param)
            if len(values) > 0:
                common.write_objects_db('import', values)
        else:
            return
    except Exception as err:
        st = f"{err}\n" + url
        print(st)


import_indicators()

# TradingEconomics('trading', 'trading').start()
# while True:
#     time.sleep(5)
