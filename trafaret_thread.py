import threading
import time
import json
import common
import common as cd
from common import get_computer_name
import config


def get_value_config_param(key, par, default=1):
    for unit in par:
        if unit['code'] == key:
            if 'is_number' not in unit or unit['is_number']:
                return int(unit['value'])
            return unit['value']
    return default


def load_config_params(name_function):
    url = "v1/view/{schema}/v_nsi_functions_params?where=name_function='{name_function}'".format(
        schema=config.SCHEMA, name_function=name_function)
    params = {"0": "id", "1": "sh_name", "2": "code", "3": "value", "4": "function", "5": "name_function",
              "6": "is_number"}
    answer, is_ok, status_code = common.send_rest(url, params=params)
    if is_ok:
        answer = json.loads(answer)
        return answer


def get_difference(caption, value_old, value):
    if value != value_old:
        return caption + ': ' + str(value_old) + ' -> ' + str(value) + '; \n'
    else:
        return ''


def get_param_work(caption, value):
    return caption + ': ' + str(value) + '; \n'


def get_difference_config_params(par, list_parameters):
    st_difference = ''
    st_param_work = ''
    for unit in list_parameters:
        for data in par:
            if unit['code'] == data['code']:
                if data['value'] != unit['value']:
                    st_difference += get_difference(unit['sh_name'], data['value'], unit['value'])
                break
        st_param_work += get_param_work(unit['sh_name'], unit['value'])
    return st_difference, st_param_work


class PatternThread(threading.Thread):
    sleep_from_now = False
    period_default = 1
    source = ''
    code_period = ''
    code_parser = ''
    description = ''
    token = ''
    lang = 'ru'
    time_begin = None  # время старта потока
    from_time = None
    next_time = 0
    par = list()
    first_cycle = True
    finish_text = ''
    st_filename = ''
    st_law_id = ''
    t0 = None  # время начала такта работы

    def __init__(self, source, code_parser, code_period='period'):
        threading.Thread.__init__(self)
        self.daemon = True
        self.source = source
        self.code_period = code_period
        self.code_parser = code_parser
        self.initiation_parameters()

    def make_next_time(self, value_minute, from_time):
        self.from_time = from_time
        self.next_time = from_time + value_minute * 60

    def define_next_time(self):
        if self.next_time is None or self.next_time == 0:
            self.next_time = time.time()
        else:
            self.make_next_time(get_value_config_param(self.code_period, self.par, self.period_default),
                                self.from_time)

    def analysis_changing_parameters(self, list_parameters):
        st_difference, st_param_work = get_difference_config_params(self.par, list_parameters)
        self.par = list_parameters
        if self.first_cycle:
            cd.write_log_db('Параметры работы', self.source, st_param_work.strip(),
                            file_name=get_computer_name() + '\n "' + self.code_parser + '"')
        if st_difference != '' and not self.first_cycle:
            cd.write_log_db(
                'Изменение параметров', self.source, st_difference.strip(),
                file_name=get_computer_name() + '\n "' + self.code_parser + '"')
            last_time = self.next_time
            self.define_next_time()
            if last_time != self.next_time:
                cd.write_log_db(
                    'Следующая активность', self.source,
                    'Старая планируемая активность в ' + time.asctime(time.gmtime(last_time)) + '\n' +
                    'Новая планируемая активность в ' + time.asctime(time.gmtime(self.next_time)),
                    file_name=get_computer_name() + '\n "' + self.code_parser + '"')
        self.first_cycle = False

    def initiation_parameters(self):
        pass

    def work(self):
        pass

    def get_duration(self):
        return common.get_duration(time.time() - self.time_begin)

    def make_login(self):
        txt, is_ok, self.token, self.lang = cd.login_superadmin()
        if not is_ok:
            cd.write_log_db('ERROR', self.source,
                            'Ошибка при login с RestAPI: ' + str(txt) + '. Ожидаем минуту',
                            file_name=get_computer_name() + '\n "' + self.code_parser + '"')
            return False
        return True

    def run(self):
        cd.write_log_db('LOAD', self.source, self.description,
                        file_name=get_computer_name() + '\n "' + self.code_parser + '"')
        self.from_time = time.time()
        self.time_begin = time.time()
        self.next_time = time.time()
        while True:
            self.t0 = time.time()
            self.finish_text = ''
            list_parameters = load_config_params(self.code_parser)  # словарь считанных параметров для сервиса
            try:
                if list_parameters is not None:
                    # анализ изменения параметров и реакция на это
                    self.analysis_changing_parameters(list_parameters)
                if time.time() >= self.next_time:  # подошло время работать
                    last_time = self.next_time
                    if not self.work():
                        self.next_time = last_time
                    else:
                        if self.sleep_from_now:
                            self.make_next_time(get_value_config_param(self.code_period, self.par), time.time())
                        else:
                            self.make_next_time(get_value_config_param(self.code_period, self.par), self.next_time)
                        while self.next_time < time.time():
                            self.from_time = self.next_time
                            self.define_next_time()
                        st = 'Тайм-аут'+cd.get_duration(get_value_config_param(
                            self.code_period, self.par) * 60) + ' до ' + time.asctime(time.gmtime(self.next_time))
                        cd.write_log_db('Finish', self.source, st + '.\n' + self.finish_text,
                                        td=time.time() - self.t0,
                                        file_name=get_computer_name() + '\n "' + self.code_parser + '"')
            except Exception as err:
                cd.write_log_db('Exception', self.source, f"{err} сервер=" + get_computer_name(),
                                td=time.time() - self.t0, file_name=self.st_filename + '\n "' + self.code_parser + '"',
                                law_id=self.st_law_id)
            time_out = 60 - (time.time() - self.t0)
            if time_out <= 0:
                time_out = 60
            time.sleep(time_out)
