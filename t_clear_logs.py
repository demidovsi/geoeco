import json
import config
import trafaret_thread
from trafaret_thread import get_value_config_param
import time
import common as cd
import datetime
import common
from common import SRC

obj = None


class ClearLogs(trafaret_thread.PatternThread):
    def __init__(self, source, code_function):
        super(ClearLogs, self).__init__(source, code_function)

    def work(self):
        super(ClearLogs, self).work()
        t0 = time.time()
        if not self.make_login():
            return False  # не удалось сделать логин (ждем минуту)
        try:
            count_days = get_value_config_param("compliment", self.par)
            if count_days <= 0:
                cd.write_log_db('❌ERROR', SRC,
                                "Ошибка в задании кол-ва дней хранения {count_days}; Повторим через минуту".format(
                                    count_days=count_days), law_id=self.code_parser + '\n' + self.source,
                                td=time.time() - t0)
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
                        ".\nКол-во дней хранения={count_days}".format(count_days=count_days)
                    return True
                cd.write_log_db('❌ERROR', SRC, 'delete_logs: неожиданный ответ ' + str(answer),
                                law_id=self.code_parser + '\n' + self.source, td=time.time() - t0)
                return False
            else:
                cd.write_log_db('❌ERROR', SRC, str(answer) + "; Повторим через минуту", law_id=self.code_parser + '\n' + self.source,
                                td=time.time() - t0)
                return False
        except Exception as er:
            cd.write_log_db('💥Exception', SRC, f"{er}; Повторим через минуту", law_id=self.code_parser + '\n' + self.source, td=time.time() - t0)
            return False

    def get_description(self):
        return 'Поток чистки лог файла'

    def get_compliment(self):
        return "Кол-во дней хранения={count_days}".format(
            count_days=int(get_value_config_param("compliment", self.par)))


if __name__ == '__main__':
    ClearLogs('Чистка лога', 'clear_logs').start()
    while True:
        time.sleep(5)
