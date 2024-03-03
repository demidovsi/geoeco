# https://pypi.org/project/wbgapi/  описание библиотеки wbgari
# Сервисы по закачке данных в базе ECO (гео-экономика)
# https://w3.unece.org/PXWeb/ru/TableDomains/
# https://www.numbeo.com/cost-of-living/prices_by_country.jsp
# https://restcountries.com/v3.1/all
# https://api.worldbank.org/v2/country/all/indicator/NY.ADJ.NNTY.KD.ZG?format=json&page=1&date=2022&per_page=10
# https://ru.tradingeconomics.com/country-list/personal-income-tax-rate
import socket
import common
import countries
import numbeo
import un
import wordbank
import cities
import government
import bigmac
import json
import time
import config
import tables_html
import tables_csv
import provinces
import turkey
import russia
import tradingeconomic
import requests
import os
from requests.utils import DEFAULT_CA_BUNDLE_PATH
import wbgapi as wb
import pandas as pd
import wikipedia
import trafaret_thread
import clear_logs

version = '1.0.1 2024-03-02'

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print(DEFAULT_CA_BUNDLE_PATH)
    # os.environ["REQUESTS_CA_BUNDLE"] = os.path.abspath("cacert.pem")
    # requests.utils.DEFAULT_CA_BUNDLE_PATH = os.path.abspath("cacert.pem")
    # requests.adapters.DEFAULT_CA_BUNDLE_PATH = os.path.abspath("cacert.pem")
    # print(os.environ["REQUESTS_CA_BUNDLE"])


    # filename = 'source/UN/gdp_import1.json'
    # f = open(filename, 'r')
    # with f:
    #     answer = f.read()
    # answer = json.loads(answer)
    # f = open(filename, 'w', encoding='utf-8')
    # with f:
    #     f.write(json.dumps(answer, indent=4, ensure_ascii=False, sort_keys=True))

    # ------------------ не задокументировано
    # ????? government.load_form_government_html(
    #     'https://merkator.org.ua/ru/spravochnik/formy-pravleniya-stran-mira', 'government')

    # cities.make_form_government('source/Формы правления государств.txt')  # формы государства
    # cities.make_type_government('source/Формы государственного устройства.txt')  # типы государства

    # bigmac.load_bm()  # !!! отдельная песня сделанная Кириллом через эксель и экспорт в свою базу

    # countries.load_courses('2024-02-10')  # загрузка курсов валюты стран за указанные сутки

    # tables_csv.load_inform('source/ourworldindata/population-change.csv', 'population_change', index_value=2)  # Общий прирост населения
    # tables_csv.load_inform('source/ourworldindata/population.csv', 'pops', index_value=2)  # Общее население
    # tables_csv.load_inform('source/ourworldindata/cost-healthy-diet.csv', 'cost_hd', index_code=1, index_year=2,
    #                        index_value=3)  # Суточная стоимость здорового питания (дол)
    # tables_csv.load_inform('source/ourworldindata/human-development-index.csv', 'hdi', index_code=1, index_year=2,
    #                        index_value=3)  # Индекс человеческого развития
    # tables_csv.load_inform('source/ourworldindata/air-passengers-carried.csv', 'air_pass_carry', index_code=1,
    #                        index_year=2, index_value=3)  # Количество перевезенных авиапассажиров
    # tables_csv.load_inform('source/ourworldindata/tourism-gdp-proportion-of-total-gdp.csv', 'tourism_gdp_per',
    #                        index_code=1, index_year=2, index_value=3)  # Количество перевезенных авиапассажиров
    # tables_csv.load_inform(
    #   'source/ourworldindata/average-length-of-stay.csv', 'av_day_of_stay',
    #   index_code=1, index_year=2, index_value=3)  # Средняя продолжительность пребывания ин. гостей

    # ?????tables_html.load_trading('https://ru.tradingeconomics.com/country-list/full-year-gdp-growth')
    # ?????tables_html.load_trading('https://api.tradingeconomics.com/country/russia')

    # countries.receive_inform()  # взять информацию из API и записать в файл result.json
    # countries.load_tld()  # суффиксы доменов
    # countries.load_languages()  # языки мира
    # countries.load_currencies() # валюты мира
    # countries.load_list_countries()  # загрузить страны для границ
    # countries.make_countries()  # прочитать не историческую информацию по странам
    # countries.load_courses(common.st_today())  # загрузить курсы валют на текущий день
    # un.load_inform()
    # wordbank.load_inform()
    # numbeo.load_inform_countries()
    # numbeo.load_inform_cities()
    # turkey.make_inform()  # отдельная обработка Турции

    # russia.load_inform()
    # pd.set_option('display.max_rows', 6)
    # for row in wb.data.fetch(['VC.IHR.PSRC.P5'], skipBlanks=True):  # all years
    #     print(row)
    # for row in wb.data.DataFrame(['VC.IHR.PSRC.P5'], time=range(2000, 2024)):  # all years
    #     print(row)
    # wordbank.load_data('FR.INR.DPST', 'deposit_rate')
    # wordbank.import_indicators()
    # wordbank.import_data('FR.INR.LEND')
    # wordbank.load_data('EP.PMP.SGAS.CD', 'gasoline_price')
    # 'NY.GNP.PCAP.PP.CD' = gnp_person
    # tradingeconomic.load_data_html('social-security-rate-for-employees')
    # turkey.sale_foreign_tourists(25)
    # wikipedia.get_corruption_index()
    # tradingeconomic.load_data_html('home-ownership-rate')

    # country, city, is_ok = common.define_guest(socket.gethostbyname(socket.gethostname()), False)

    common.write_log_db(
        'START', 'WebParser', 'Старт сервиса подкачки статистической информации \n' +
        '; version: ' + version +
        '; host: ' + config.URL +
        '; schema: ' + config.SCHEMA,
        file_name=common.get_computer_name()
    )

    wordbank.obj = wordbank.Wb('Всемирный банк', 'wb')
    wordbank.obj.start()

    clear_logs.obj = clear_logs.ClearLogs('Чистка лога', 'clear_logs')
    clear_logs.obj.start()

    while True:
        time.sleep(5)
