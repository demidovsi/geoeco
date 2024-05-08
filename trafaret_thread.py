import threading
import time
import datetime
import json
import common
import common as cd
from common import get_computer_name
import config


def get_value_config_param(key, par, default=1):
    if key in par and par[key]:
        return par[key]
    return default


def load_config_params(name_function):
    url = "v1/select/{schema}/nsi_parser_functions?where=sh_name='{name_function}'".format(
        schema=config.SCHEMA, name_function=name_function)
    answer, is_ok, status_code = common.send_rest(url)
    if is_ok and len(answer) > 0:
        answer = json.loads(answer)[0]
        return answer


def get_difference(caption, value_old, value):
    if value != value_old:
        return caption + ': ' + str(value_old) + ' -> ' + str(value) + '; \n'
    else:
        return ''


def get_param_work(caption, value, comment=''):
    return caption + ': ' + str(value) + comment + '; \n'


def get_difference_config_params(par, list_parameters):
    st_difference = ''
    if 'active' in par:
        st_difference += get_difference(list_parameters['sh_name'], list_parameters['active'], par['active'])
    if 'period' in par:
        st_difference += get_difference(list_parameters['sh_name'], list_parameters['period'], par['period'])

    st_param_work = ''
    st_param_work += get_param_work('active', list_parameters['active'])
    if float(list_parameters['period']) < 0:
        st_param_work += get_param_work('period', str(abs(float(list_parameters['period']))), ' minutes')
    else:
        st_param_work += get_param_work('period', list_parameters['period'], ' days')

    if list_parameters['at_date_time']:
        st_param_work += get_param_work('at_date_time', list_parameters['at_date_time'].replace('T', ' '))
    return st_difference, st_param_work


def set_value_config_param(key, par, value, token=None):
    if token is None:
        answer, is_ok, token_admin, lang_admin = common.login_superadmin()
        if not is_ok:
            print('ERROR', 'login', str(answer) + '.\n Для ' + par['sh_name'] + ' ' + key + '=' + str(value))
            return is_ok
    params = {"values": {key: "'{value}'".format(value=value)}}
    answer, is_ok, status = common.send_rest(
        'v1/update/{schema}/{table}?where=id={id}'.format(
            schema=config.SCHEMA, table='nsi_parser_functions', id=par['id']),
        'PATCH', params=params, token_user=token)
    if not is_ok:
        common.write_log_db(
            'ERROR', 'update_parser_functions',
            str(answer) + '.\n Для ' + par['sh_name'] + ' ' + key + '=' + str(value), token_admin=token)
    return is_ok


class PatternThread(threading.Thread):
    period_default = 1
    source = ''
    code_period = ''
    code_parser = ''
    description = ''
    token = None
    lang = 'ru'
    time_begin = None  # время старта потока
    from_time = None
    next_time = 0
    time_start = None  # время начала такта работы
    par = dict()
    first_cycle = True
    finish_text = ''
    st_filename = ''
    st_law_id = ''
    list_start_metric = list()

    def __init__(self, source, code_parser):
        threading.Thread.__init__(self)
        self.daemon = True
        self.source = source
        self.code_parser = code_parser
        self.initiation_parameters()

    def make_next_time(self, value_day, from_time):
        self.from_time = from_time
        if value_day < 0:
            self.next_time = from_time + abs(value_day) * 60
        else:
            self.next_time = from_time + value_day * 86400

    def define_next_time(self):
        if self.next_time is None or self.next_time == 0:
            self.next_time = time.mktime(time.gmtime())
        else:
            self.make_next_time(get_value_config_param('period', self.par, self.period_default),
                                self.from_time)

    def analysis_changing_parameters(self, list_parameters):
        st_difference, st_param_work = get_difference_config_params(self.par, list_parameters)
        self.par = list_parameters
        n = get_value_config_param('period', self.par)
        n = abs(n) * 60 if n < 0 else n * 86400
        st_first = time.asctime(time.gmtime())
        if list_parameters['at_date_time'] and self.first_cycle:
            dt = datetime.datetime.strptime(list_parameters['at_date_time'], "%Y-%m-%dT%H:%M:%S")
            self.next_time = dt.timestamp() + n
            if time.mktime(time.gmtime()) >= self.next_time:
                self.next_time = time.mktime(time.gmtime())
            st_first = time.ctime(self.next_time)
        if self.first_cycle:
            cd.write_log_db(
                'Параметры работы', self.source,
                st_param_work.strip() + ' ' + self.get_compliment() + '; факт старт в ' + st_first,
                file_name=get_computer_name() + '\n поток="' + self.code_parser + '"',
                token_admin=self.token)
        if st_difference != '' and not self.first_cycle:
            cd.write_log_db(
                'Изменение параметров', self.source, st_difference.strip(),
                file_name=get_computer_name() + '\n поток="' + self.code_parser + '"')
            last_time = self.next_time
            self.define_next_time()
            if last_time != self.next_time:
                cd.write_log_db(
                    'Следующая активность', self.source,
                    'Старая планируемая активность в ' + time.asctime(time.gmtime(last_time)) + '\n' +
                    'Новая планируемая активность в ' + time.asctime(time.gmtime(self.next_time)),
                    file_name=get_computer_name() + '\n поток="' + self.code_parser + '"',
                    token_admin=self.token)
        # if list_parameters['at_date_time'] and self.first_cycle:
        #     dt = datetime.datetime.strptime(list_parameters['at_date_time'], "%Y-%m-%dT%H:%M:%S")
        #     self.make_next_time(get_value_config_param('period', self.par, self.period_default), dt.timestamp())
        self.first_cycle = False

    def initiation_parameters(self):
        self.par = load_config_params(self.code_parser)

    def work(self):
        pass

    def check_import_metric(self):
        # выполнение после каждой работы или тайм-аута
        self.list_start_metric = []
        answer, is_ok, status = common.send_rest(
            "v1/select/{schema}/list_start_metric?where=sh_name='{sh_name}'".format(
                schema=config.SCHEMA, sh_name=self.code_parser))
        if not is_ok:
            cd.write_log_db('Error', self.source, str(answer) + "; сервер= + get_computer_name()",
                            file_name='поток="' + self.code_parser + '"', token_admin=self.token)
            return
        answer = json.loads(answer)
        for data in answer:
            self.list_start_metric.append(data['indicator'])
        if len(self.list_start_metric) != 0:
            sql_query = "delete from {schema}.list_start_metric where sh_name='{sh_name}'; select 1;".format(
                schema=config.SCHEMA, sh_name=self.code_parser)
            common.write_script_db(sql_query, token=self.token)

    def decode_finish_one_indicator(self, data, count_row, st_absent, t0):
        if count_row and count_row != 0:
            st = data['name_rus']
            if st_absent:
                name_absent = common.get_name_object(data)
                st += ';\n не найдены ' + name_absent + ': "' + st_absent + '"'
            common.write_log_db(
                'import', self.source + ' (по запросу)', st, page=count_row, td=time.time() - t0,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser +
                '"; param_name="' + data['param_name'] + '; ' + data['object_code'] + '"',
                token_admin=self.token)

    def load_indicators(self, active=None):
        url = "v1/select/{schema}/nsi_import?where=sh_name='{code_function}'".format(
            schema=config.SCHEMA, code_function=self.code_parser)
        url = url + ' and active ' if active else url
        answer, is_ok, status = common.send_rest(url)
        if not is_ok:
            common.write_log_db(
                'error', self.source, str(answer) + '; ' + url,
                file_name=common.get_computer_name() + '\n поток="' + self.code_parser, token_admin=self.token)
            return
        answer = json.loads(answer)
        return answer

    def get_duration(self):
        return common.get_duration(time.mktime(time.gmtime()) - self.time_begin)

    def make_login(self):
        txt, is_ok, self.token, self.lang = cd.login_superadmin()
        if not is_ok:
            print('ERROR', self.source,
                  'Ошибка при login с RestAPI: ' + str(txt) + '. Ожидаем минуту',
                  get_computer_name() + '\n поток="' + self.code_parser + '"')
            return False
        return True

    def get_description(self):
        return self.description

    def get_compliment(self):
        return ''

    def run(self):
        cd.write_log_db('RUN', self.source, self.get_description(),
                        file_name=get_computer_name() + '\n поток="' + self.code_parser + '"',
                        token_admin=self.token)
        self.from_time = time.mktime(time.gmtime())
        self.time_begin = time.mktime(time.gmtime())
        self.next_time = time.mktime(time.gmtime())
        while True:
            time0 = time.mktime(time.gmtime())
            self.finish_text = ''
            list_parameters = load_config_params(self.code_parser)  # словарь считанных параметров для сервиса
            try:
                if list_parameters is not None:
                    if self.par['active'] != list_parameters['active'] and list_parameters['active'] == 0:
                        self.first_cycle = True
                    self.analysis_changing_parameters(list_parameters)
                    # анализ изменения параметров и реакция на это
                if time.mktime(time.gmtime()) >= self.next_time:  # подошло время работать
                    self.time_start = time.mktime(time.gmtime())
                    last_time = self.next_time
                    sleep = get_value_config_param('active', self.par) != 1
                    if sleep:
                        common.write_log_db('SLEEP', self.source, 'Поток не АКТИВЕН (active)',
                                            file_name=get_computer_name(), token_admin=self.token)
                    else:
                        sleep = not self.make_login()  # нет логина - ждать
                    if sleep or not self.work():
                        self.next_time = last_time
                    else:
                        n = get_value_config_param('period', self.par)
                        n = abs(n) * 60 if n < 0 else n * 86400
                        # self.from_time = self.time_start
                        self.from_time = self.next_time
                        self.next_time = self.time_start + n
                        st = 'Тайм-аут ' + cd.get_duration(n) + ' до ' + time.ctime(self.next_time)
                        cd.write_log_db('Finish', self.source, st + '.\n' + self.finish_text,
                                        td=time.mktime(time.gmtime()) - self.time_start,
                                        file_name=get_computer_name() + '\n поток="' + self.code_parser + '"',
                                        token_admin=self.token)
                        set_value_config_param('at_date_time', self.par, common.st_now(), token=self.token)
            except Exception as err:
                cd.write_log_db('Exception', self.source, f"{err} сервер=" + get_computer_name(),
                                td=time.mktime(time.gmtime()) - time0,
                                file_name=self.st_filename + '\n поток="' + self.code_parser + '"',
                                law_id=self.st_law_id, token_admin=self.token)
            self.check_import_metric()
            time_out = 60 - (time.mktime(time.gmtime()) - time0)
            if time_out <= 0:
                time_out = 60
            # if self.code_parser == 'meteo':
            #     print('Сейчас='+time.ctime(), 'Время запуска='+time.ctime(self.next_time), 'time-out', time_out)
            time.sleep(time_out)
