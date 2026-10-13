"""
geo_server — сервис подкачки статистической информации в БД ECO (гео-экономика).

Назначение
    Периодически собирает экономические, демографические и справочные данные по странам,
    провинциям (штатам) и городам из внешних источников и записывает их в PostgreSQL
    (схема config.SCHEMA, по умолчанию eco). Работа с БД идёт только через RestAPI
    проекта RESTproxy (config.URL): чтение — v1/select, запись — v2/execute (SQL-функции
    pw_his*, pw_his_cities, pw_his_provinces), справочники — v1/objects, лог — pw_logs.

Устройство
    Каждый источник — отдельный поток-наследник trafaret_thread.PatternThread. Поток раз в
    минуту перечитывает свои параметры из таблицы nsi_parser_functions (active, period,
    at_date_time, compliment / compliment_txt) и, когда подходит время, выполняет work():
        period > 0 — интервал в сутках, period < 0 — интервал в минутах;
        active != 1 — поток спит; at_date_time — время последнего успешного запуска.
    Список загружаемых индикаторов берётся из nsi_import (sh_name = код потока).
    Внеочередная загрузка отдельного индикатора — запись в list_start_metric.
    Ход работы (старт, импорт, ошибки, тайм-ауты) пишется в лог БД через common.write_log_db.

Потоки (код потока в nsi_parser_functions)
    un          — t_unece: таблицы ЕЭК ООН (w3.unece.org) по странам
    wb          — t_worldbank: индикаторы Всемирного банка (библиотека wbgapi)
    numbeo      — t_numbeo: стоимость жизни по странам и городам (numbeo.com) — отключён
    trading     — t_tradingeconomics: показатели стран с tradingeconomics.com,
                  при первом запуске пополняет список индикаторов в nsi_import
    clear_logs  — t_clear_logs: удаление записей лога старше compliment дней
    courses     — t_courses: курсы валют к USD за вчерашний день (apilayer)
    meteo       — t_meteo: текущая погода по городам (OpenWeatherMap) и расчёт
                  среднесуточных значений в avr_cities_meteo_day
    usa         — t_usa: население и площадь штатов США, справочник штатов и столиц (Википедия)
    Отключены: statdata, countries, wikipedia, indonesia, spain, cyprus, georgia.

Запуск и деплой
    python main_geo_server.py — настройки и секреты читаются из .env (шаблон .env.example).
    Остановка по SIGTERM/Ctrl+C с записью shutdown в лог.
    При push в master GitHub Actions собирает Docker-образ ghcr.io/demidovsi/geoeco и
    перезапускает контейнер geoeco на 159.223.0.234 (секреты — /opt/geoeco/.env).

Источники
    https://pypi.org/project/wbgapi/  описание библиотеки wbgapi
"""
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

version = 'version 1.6 2026-10-13'

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

        # t_numbeo.Numbeo('Numbeo', 'numbeo').start()  # кончилась подписка

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
