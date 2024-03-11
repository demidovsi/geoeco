import json
import config
import trafaret_thread
from trafaret_thread import get_value_config_param
import time
import common as cd
import datetime
import common

obj = None


class ClearLogs(trafaret_thread.PatternThread):
    def __init__(self, source, code_function):
        super(ClearLogs, self).__init__(source, code_function)

    def work(self):
        super(ClearLogs, self).work()
        if not self.make_login():
            return False  # не удалось сделать логин (ждем минуту)
        try:
            count_days = get_value_config_param("compliment", self.par)
            if count_days <= 0:
                cd.write_log_db('ERROR', self.source,
                                "Ошибка в задании кол-ва дней хранения {count_days}; Повторим через минуту".format(
                                    count_days=count_days),
                                td=time.time() - self.t0)
                return False
            first_date = datetime.date.today() - datetime.timedelta(
                days=count_days)
            last_date = str(first_date.year) + '-' + str(first_date.month).zfill(2) + '-' + str(first_date.day).zfill(2)
            answer, is_ok, status = common.send_rest(
                "v1/function/{schema}/delete_logs?text='{text}'&view=0".format(
                    schema=config.SCHEMA, text=last_date), 'POST', token_user=self.token)
            if is_ok:
                answer = json.loads(answer)
                if 'count_before' in answer:
                    start_count = answer['count_before']
                    finish_count = answer['count_after']
                    self.finish_text = ' Было строк ' + str(start_count) + '. Стало строк ' + str(finish_count) + \
                        ". Кол-во дней хранения={count_days}".format(count_days=count_days)
                    return True
            else:
                cd.write_log_db('ERROR', self.source, str(answer) + "; Повторим через минуту",
                                td=time.time() - self.t0)
                return False
        except Exception as er:
            cd.write_log_db('Exception', self.source, f"{er}; Повторим через минуту", td=time.time() - self.t0)
            return False

    def get_description(self):
        return 'Поток чистки лог файла'

    def get_compliment(self):
        return "Кол-во дней хранения={count_days}".format(
            count_days=int(get_value_config_param("compliment", self.par)))
