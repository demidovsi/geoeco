# https://pypi.org/project/wbgapi/  описание библиотеки wbgari
# Сервисы по закачке данных в базе ECO (гео-экономика)
# https://w3.unece.org/PXWeb/ru/TableDomains/
# https://www.numbeo.com/cost-of-living/prices_by_country.jsp
# https://restcountries.com/v3.1/all
# https://api.worldbank.org/v2/country/all/indicator/NY.ADJ.NNTY.KD.ZG?format=json&page=1&date=2022&per_page=10
# https://ru.tradingeconomics.com/country-list/personal-income-tax-rate
import signal
import threading
import common
from common import SRC
import config
import t_worldbank
import t_numbeo
import t_clear_logs
import t_tradingeconomics
import t_courses
import t_meteo
import t_unece
import t_usa
# Отключённые потоки (при включении добавить соответствующий import):
# t_statdata, t_countries, t_wikipedia, t_indonesia, t_spain, t_cyprus, t_georgia

version = 'version 1.4 2026-10-13'

stop_event = threading.Event()


def _on_signal(signum, frame):
    stop_event.set()


def server_info():
    return 'host: ' + config.URL + '; schema: ' + config.SCHEMA


if __name__ == '__main__':
    # ------------------ не задокументировано
    # bigmac.load_bm()  # !!! отдельная песня сделанная Кириллом через эксель и экспорт в свою базу
    # turkey.make_inform()  # отдельная обработка Турции
    # russia.load_inform()
    # turkey.sale_foreign_tourists(25)

    # SIGTERM приходит от systemd при остановке/рестарте сервиса
    signal.signal(signal.SIGTERM, _on_signal)
    signal.signal(signal.SIGINT, _on_signal)

    common.write_log_db(
        '🔥startup', SRC, 'Старт сервиса подкачки статистической информации;\n' + server_info(),
        law_id=version
    )

    try:
        t_unece.Unece('ООН', 'un').start()
        # t_statdata.StatData('Крупные города', 'statdata').start()
        # t_countries.Countries('Описание стран', 'countries').start()
        t_worldbank.Wb('Всемирный банк', 'wb').start()
        t_numbeo.Numbeo('Numbeo', 'numbeo').start()
        t_tradingeconomics.TradingEconomics('Tradingeconomics', 'trading').start()
        # t_wikipedia.Wikipedia('Википедия', 'wikipedia').start()
        # t_indonesia.Indonesia('Индонезия', 'indonesia').start()
        # t_spain.Spain('Испания', 'spain').start()
        t_clear_logs.ClearLogs('Чистка лога', 'clear_logs').start()
        t_courses.Courses('Курсы валют', 'courses').start()
        t_meteo.Meteo('Метео по городам', 'meteo').start()
        # t_cyprus.Cyprus('Кипр', 'cyprus').start()
        # t_georgia.Georgia('Грузия', 'georgia').start()
        t_usa.Usa('США', 'usa').start()

        # ожидание с таймаутом, чтобы Ctrl+C срабатывал и под Windows
        while not stop_event.wait(5):
            pass
        common.write_log_db(
            '🛑shutdown', SRC, 'Сервис остановлен (сигнал остановки);\n' + server_info(),
            law_id=version
        )
    except Exception as err:
        common.write_log_db(
            '💥shutdown', SRC, 'Аварийное завершение сервиса:\n' + f'{err};\n' + server_info(),
            law_id=version
        )
        raise SystemExit(1)
