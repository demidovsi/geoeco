import trafaret_thread
import common
from common import SRC
import config
import time
import wbgapi as wb

obj = None


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
                        count_row, st_absent, max_year, _ = self.import_data(data, countries=countries)
                        self.decode_finish_one_indicator(data, count_row, st_absent, t0)

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
                count_row, st_absent, max_year, is_ok = self.import_data(data, countries=countries)
                own = f"\n{index}/{len(answer)}"
                if is_ok and count_row and count_row != 0:
                    result += count_row
                    st = data['name_rus']
                    if st_absent:
                        st += ';\n не найдены страны: "' + st_absent + '"'
                    common.write_log_db(
                        '✔️import', SRC, st + '\nparam_name="' + data['param_name'] + '; ' + data['object_code'] +
                        f'\nmax_year={max_year}',
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
            return None, '', '', False
        if data['param_name'] is None:
            print('Не задано имя параметра для', data['code'])
            return None, '', '', False
        if countries is None:
            countries = common.load_countries(self.token)
        if countries is None:
            print('Отсутствуют страны')
            return None, '', '', False
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
                common.write_script_db(st_query, token=self.token)
                return len(list_country), st_absent, max_year, True
            except Exception as err:
                err_msg = f"{err}"
                if 'not found' in err_msg.lower() or 'not valid' in err_msg.lower() or 'json decod' in err_msg.lower():
                    common.write_log_db('⚠️Warning', SRC, f"Данные недоступны: {err_msg}\n" + data['name_rus']+
                                        '\nparam_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                                        law_id=self.code_parser + '\n' + self.source, token_admin=self.token)
                    return 0, err_msg, '', True
                common.write_log_db('💥Exception', SRC, f"attempt {attempt + 1}/3: {err_msg}\n" + data['name_rus']+
                                    '\nparam_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                                    law_id=self.code_parser + '\n' + self.source, token_admin=self.token)
                if attempt < 2:
                    time.sleep(10)
                    st_query = ''
                    list_country = []
                    max_year = ''
        return 0, err_msg, '', False


if __name__ == '__main__':
    Wb('Всемирный банк', 'wb').start()
    while True:
        time.sleep(5)
