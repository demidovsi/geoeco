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
    url = "v1/view/{schema}/v_nsi_parser_functions?where=sh_name='{name_function}'".format(
        schema=config.SCHEMA, name_function=name_function)
    params = {"0": "id", "1": "sh_name", "2": "description", "3": "active", "4": "period", "5": "compliment",
              "6": "at_date_time"}
    answer, is_ok, status_code = common.send_rest(url, params=params)
    if is_ok:
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

    def __init__(self, source, code_parser):
        threading.Thread.__init__(self)
        self.daemon = True
        self.source = source
        self.code_parser = code_parser
        self.initiation_parameters()

    def make_next_time(self, value_day, from_time):
        self.from_time = from_time
        self.next_time = from_time + value_day * 86400

    def define_next_time(self):
        if self.next_time is None or self.next_time == 0:
            self.next_time = time.time()
        else:
            self.make_next_time(get_value_config_param('period', self.par, self.period_default),
                                self.from_time)

    def analysis_changing_parameters(self, list_parameters):
        st_difference, st_param_work = get_difference_config_params(self.par, list_parameters)
        self.par = list_parameters
        if self.first_cycle:
            cd.write_log_db('Параметры работы', self.source, st_param_work.strip() + ' ' + self.get_compliment(),
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
        if list_parameters['at_date_time'] and self.first_cycle:
            dt = datetime.datetime.strptime(list_parameters['at_date_time'], "%Y-%m-%dT%H:%M:%S")
            self.make_next_time(get_value_config_param('period', self.par, self.period_default), dt.timestamp())
            # cd.write_log_db(
            #     'Коррекция активности', self.source,
            #     'Старая планируемая активность в ' + time.ctime(self.from_time) + '\n' +
            #     'Новая планируемая активность в ' + time.ctime(self.next_time),
            #     file_name=get_computer_name() + '\n поток="' + self.code_parser + '"',
            #     token_admin=self.token)
        self.first_cycle = False

    def initiation_parameters(self):
        self.par = load_config_params(self.code_parser)

    def work(self):
        pass

    def get_duration(self):
        return common.get_duration(time.time() - self.time_begin)

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
        self.from_time = time.time()
        self.time_begin = time.time()
        self.next_time = time.time()
        while True:
            self.time_start = time.time()
            self.finish_text = ''
            list_parameters = load_config_params(self.code_parser)  # словарь считанных параметров для сервиса
            try:
                if list_parameters is not None:
                    if self.par['active'] != list_parameters['active'] and list_parameters['active'] == 0:
                        self.first_cycle = True
                    self.analysis_changing_parameters(list_parameters)
                    # анализ изменения параметров и реакция на это
                if time.time() >= self.next_time:  # подошло время работать
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
                        self.from_time = self.time_start
                        self.next_time = self.time_start + get_value_config_param('period', self.par) * 86400
                        st = 'Тайм-аут ' + cd.get_duration(get_value_config_param(
                            'period', self.par) * 86400) + ' до ' + time.asctime(time.gmtime(self.next_time))
                        cd.write_log_db('Finish', self.source, st + '.\n' + self.finish_text,
                                        td=time.time() - self.time_start,
                                        file_name=get_computer_name() + '\n поток="' + self.code_parser + '"',
                                        token_admin=self.token)
                        set_value_config_param('at_date_time', self.par, common.st_now(), token=self.token)
            except Exception as err:
                cd.write_log_db('Exception', self.source, f"{err} сервер=" + get_computer_name(),
                                td=time.time() - self.time_start,
                                file_name=self.st_filename + '\n поток="' + self.code_parser + '"',
                                law_id=self.st_law_id, token_admin=self.token)
            time_out = 60 - (time.time() - self.time_start)
            if time_out <= 0:
                time_out = 60
            time.sleep(time_out)
