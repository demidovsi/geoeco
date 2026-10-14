import trafaret_thread
import common
from common import SRC
import config
import time
import requests
import wbgapi as wb

obj = None

# Индикаторы, удалённые из WDI (источник 2), Всемирный банк переносит в архив «WDI Database Archives»
# (источник 57). В архиве у данных есть измерение «версия выпуска» (с 1989 г.), поэтому данные берутся
# из последней версии, где у индикатора есть значения.
ARCHIVE_URL = 'https://api.worldbank.org/v2/sources/57'
_archive_version = {}  # код индикатора -> версия архива с данными (None — в архиве данных нет)


def _get_json(url):
    r = requests.get(url, timeout=(15, 180))
    r.raise_for_status()
    return r.json()


def _archive_rows(code, version):
    """Непустые значения индикатора в версии архива: список (economy, year, value)."""
    rows = []
    page, pages = 1, 1
    while page <= pages:
        d = _get_json('{url}/series/{code}/version/{version}/country/all/time/all?format=json&per_page=20000&page={page}'.
                      format(url=ARCHIVE_URL, code=code, version=version, page=page))
        if not isinstance(d, dict):
            break  # индикатора нет и в архиве
        pages = d.get('pages', 1)
        for item in d.get('source', {}).get('data', []):
            if item['value'] is None:
                continue
            var = {v['concept']: v for v in item['variable']}
            rows.append((var['Country']['id'], var['Time']['value'], item['value']))
        page += 1
    return rows


def fetch_archive(code):
    """
    Данные индикатора из последней версии архива WDI, где они есть.
    Найденная версия кешируется до перезапуска сервиса, чтобы не перебирать архив каждый такт.
    :return: (версия, [(economy, year, value)]) или (None, []), если в архиве данных нет
    """
    if code in _archive_version:
        version = _archive_version[code]
        return (version, _archive_rows(code, version)) if version else (None, [])
    versions = _get_json(ARCHIVE_URL + '/version?format=json&per_page=1000')['source'][0]['concept'][0]['variable']
    last_in_year = {}
    for v in versions:  # версии упорядочены по возрастанию — остаётся последняя версия каждого года
        last_in_year[v['id'][:4]] = v['id']
    for year in sorted(last_in_year, reverse=True):
        version = last_in_year[year]
        rows = _archive_rows(code, version)
        if rows:
            _archive_version[code] = version
            return version, rows
    _archive_version[code] = None
    return None, []


class Wb(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(Wb, self).__init__(source, code_function)

    def work(self):
        super(Wb, self).work()
        return self.load_list_indicators()

    def check_import_metric(self):
        super(Wb, self).check_import_metric()
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
                    if data['param_name'] == indicator:
                        count_row, st_absent, max_year, _, new_rows = self.import_data(data, countries=countries)
                        self.decode_finish_one_indicator(data, count_row, st_absent, t0, new_rows)

    def load_list_indicators(self):
        result = 0
        answer = self.load_indicators(True)
        if answer:
            index = 0
            countries = common.load_countries(self.token)
            if countries is None:
                return False
            for data in answer:
                index += 1
                if data['object_code'] != 'countries':
                    continue
                t0 = time.time()
                count_row, st_absent, max_year, is_ok, new_rows = self.import_data(data, countries=countries)
                own = f"\n{index}/{len(answer)}"
                if is_ok and count_row and count_row != 0:
                    result += count_row
                    self.add_new_rows(new_rows)
                    st = data['name_rus']
                    if st_absent:
                        st += ';\n не найдены страны: "' + st_absent + '"'
                    common.write_log_db(
                        '✔️import', SRC, st + '\nparam_name="' + data['param_name'] + '; ' + data['object_code'] +
                        f'\nmax_year={max_year}' + common.st_new_rows(new_rows),
                        page=count_row, td=time.time() - t0,
                        law_id=self.code_parser + '\n' + self.source + own)
                if not is_ok:
                    common.write_log_db(
                        '❌Exception', SRC, data['name_rus'] + '; ' + st_absent,
                        td=time.time() - t0,
                        law_id=self.code_parser + '\n' + self.source + own)
        return result != 0

    def import_data(self, data, countries=None):
        """
        Импорт данных по индикатору и запись их в БД. Код параметра и индикатор задаются в nsi_import.
        :param data:
        :param countries:
        :return:
        Индикатор запрашиваемой информации находится в data['code']
        Код параметра в БД находится в data['param_name']
        """
        if data['code'] is None:
            print('Не задан индикатор параметра')
            return None, '', '', False, None
        if data['param_name'] is None:
            print('Не задано имя параметра для', data['code'])
            return None, '', '', False, None
        if countries is None:
            countries = common.load_countries(self.token)
        if countries is None:
            print('Отсутствуют страны')
            return None, '', '', False, None
        st_query = ''
        st_absent = ''
        list_country = list()
        max_year = ''
        err_msg = ''
        for attempt in range(3):
            try:
                for row in wb.data.fetch([data['code']], skipBlanks=True):  # all years
                    if row['aggregate']:
                        continue  # агрегированные данные пропускаем
                    country_id = common.get_country_id(None, countries, code=row['economy'], pr=False)
                    if country_id:
                        if country_id not in list_country:
                            list_country.append(country_id)
                        date = row['time'][2:]
                        if date > max_year:
                            max_year = date
                        st_query += "select {schema}.pw_his('{param_name}', '{date}-12-01', {country_id}, {value});\n". \
                            format(param_name=data['param_name'], date=date, country_id=country_id, value=row['value'],
                                   schema=config.SCHEMA)
                    else:
                        if row['economy'] not in st_absent:
                            st_absent = st_absent + ', ' if st_absent else st_absent
                            st_absent += row['economy']
                _, new_rows = common.write_his_db(st_query, 'his_countries_' + data['param_name'], token=self.token)
                return len(list_country), st_absent, max_year, True, new_rows
            except Exception as err:
                err_msg = f"{err}"
                if 'not found' in err_msg.lower() or 'not valid' in err_msg.lower() or 'json decod' in err_msg.lower():
                    result = self.import_from_archive(data, countries)
                    if result is not None:
                        return result
                    common.write_log_db('⚠️Warning', SRC, f"Данные недоступны: {err_msg}\n" + data['name_rus']+
                                        '\nparam_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                                        law_id=self.code_parser + '\n' + self.source, token_admin=self.token)
                    return 0, err_msg, '', True, None
                common.write_log_db('💥Exception', SRC, f"attempt {attempt + 1}/3: {err_msg}\n" + data['name_rus']+
                                    '\nparam_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                                    law_id=self.code_parser + '\n' + self.source, token_admin=self.token)
                if attempt < 2:
                    time.sleep(10)
                    st_query = ''
                    list_country = []
                    max_year = ''
        return 0, err_msg, '', False, None

    def import_from_archive(self, data, countries):
        """
        Загрузка индикатора, удалённого из WDI, из архива WDI Database Archives.
        :return: результат как у import_data или None, если в архиве данных нет / архив недоступен
        """
        t0 = time.time()
        try:
            version, rows = fetch_archive(data['code'])
        except Exception as err:
            common.write_log_db('💥Exception', SRC, f"Архив WDI: {err}\n" + data['name_rus'] +
                                '\nparam_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                                td=time.time() - t0, law_id=self.code_parser + '\n' + self.source,
                                token_admin=self.token)
            return None
        if not rows:
            return None
        st_query = ''
        list_country = list()
        max_year = ''
        for economy, year, value in rows:
            # коды агрегатов (регионы, группы доходов) в справочнике стран отсутствуют — пропускаем молча
            country_id = common.get_country_id(None, countries, code=economy, pr=False)
            if country_id is None:
                continue
            if country_id not in list_country:
                list_country.append(country_id)
            if year > max_year:
                max_year = year
            st_query += "select {schema}.pw_his('{param_name}', '{year}-12-01', {country_id}, {value});\n". \
                format(param_name=data['param_name'], year=year, country_id=country_id, value=value,
                       schema=config.SCHEMA)
        _, new_rows = common.write_his_db(st_query, 'his_countries_' + data['param_name'], token=self.token)
        common.write_log_db('ℹ️archive', SRC, 'Индикатор удалён из WDI, загружен из архива WDI Database Archives, '
                            'версия ' + version + '\n' + data['name_rus'] +
                            '\nparam_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                            td=time.time() - t0, law_id=self.code_parser + '\n' + self.source,
                            token_admin=self.token)
        return len(list_country), '', max_year, True, new_rows


if __name__ == '__main__':
    Wb('Всемирный банк', 'wb').start()
    while True:
        time.sleep(5)
