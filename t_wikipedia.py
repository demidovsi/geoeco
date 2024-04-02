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


class Wikipedia(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Wikipedia, self).__init__(source, code_function)

    def work(self):
        super(Wikipedia, self).work()
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
            countries = common.load_countries(self.token)
            if countries is None:
                print('Отсутствуют страны')
                return False
            for data in answer:
                index += 1
                count_row = 0
                if data['param_name'] == 'corruption_index':
                    count_row = self.get_corruption_index(data, countries)
                if count_row and count_row != 0:
                    result += count_row
        return result != 0

    def get_corruption_index(self, elem, countries):
        """
        Определение индекса коррупции для стран
        :param elem:
        :param countries:
        :return:
        """
        t0 = time.time()
        lws = self.load_html(elem['code'])
        if lws:
            lws = BeautifulSoup(lws, 'html.parser').find_all('table', class_="wikitable sortable zebra")[0]. \
                find_all('tbody')[0].find_all('tr')
        if lws is None:
            return None, None
        st_years = lws[1].text.strip().split('\n')
        years = list()
        for year in st_years:
            if year:
                years.append(year.strip().split('[')[0])
        lws = lws[2:]
        st_query = ''
        count = 0
        st_absent = ''
        for data in lws:
            unit = data.find_all('td')
            country_id = common.get_country_id(unit[1].text, countries, pr=False)
            if country_id:
                for i in range(2, len(unit)):
                    if unit[i].text.strip() != 'Н/Д':
                        count += 1
                        st_query += "select {schema}.pw_his('{param_name}', '{date}', {country_id}, {value});\n". \
                            format(param_name=elem['param_name'], date=years[i-2] + '-12-31', country_id=country_id,
                                   value=unit[i].text.strip(), schema=config.SCHEMA)
            else:
                if country_id is None and unit[1].text not in st_absent:
                    st_absent = st_absent + ', ' if st_absent else st_absent
                    st_absent += unit[1].text
        if common.write_script_db(st_query, token=self.token):
            st = elem['name_rus']
            if st_absent:
                st += ';\n не найдены страны: "' + st_absent + '"'
            common.write_log_db(
                'import', self.source, st, page=count, td=time.time() - t0,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser + '"; param_name="' +
                elem['param_name'] + '; ' + elem['object_code'] + '"', token_admin=self.token)
        return count
