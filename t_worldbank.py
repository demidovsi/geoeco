import trafaret_thread
import common
import config
import json
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
                        count_row, st_absent = self.import_data(data, countries=countries)
                        self.decode_finish_one_indicator(data, count_row, st_absent, t0)

    def load_list_indicators(self):
        result = 0
        answer = self.load_indicators(True)
        if answer:
            index = 0
            answer = json.loads(answer)
            countries = common.load_countries(self.token)
            if countries is None:
                return False
            for data in answer:
                index += 1
                if data['object_code'] != 'countries':
                    continue
                t0 = time.time()
                count_row, st_absent = self.import_data(data, countries=countries)
                if count_row and count_row != 0:
                    result += count_row
                    st = data['name_rus']
                    if st_absent:
                        st += ';\n не найдены страны: "' + st_absent + '"'
                    common.write_log_db(
                        'import', self.source, st, page=count_row, td=time.time() - t0,
                        law_id=str(index) + ' (всего=' + str(len(answer)) + ')',
                        file_name=common.get_computer_name() + '\n поток="' + self.code_parser +
                        '"; param_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                        token_admin=self.token)
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
            return None, ''
        if data['param_name'] is None:
            print('Не задано имя параметра для', data['code'])
            return None, ''
        if countries is None:
            countries = common.load_countries(self.token)
        if countries is None:
            print('Отсутствуют страны')
            return None, ''
        st_query = ''
        st_absent = ''
        list_country = list()
        for row in wb.data.fetch([data['code']], skipBlanks=True):  # all years
            if row['aggregate']:
                continue  # агрегированные данные пропускаем
            country_id = common.get_country_id(None, countries, code=row['economy'], pr=False)
            if country_id:
                if country_id not in list_country:
                    list_country.append(country_id)
                date = row['time'][2:]
                st_query += "select {schema}.pw_his('{param_name}', '{date}-12-01', {country_id}, {value});\n". \
                    format(param_name=data['param_name'], date=date, country_id=country_id, value=row['value'],
                           schema=config.SCHEMA)
            else:
                if row['economy'] not in st_absent:
                    st_absent = st_absent + ', ' if st_absent else st_absent
                    st_absent += row['economy']
        common.write_script_db(st_query, token=self.token)
        return len(list_country), st_absent

# Wb('Всемирный банк', 'wb').start()
# while True:
#     time.sleep(5)
