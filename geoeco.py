# https://pypi.org/project/wbgapi/  описание библиотеки wbgari
# Сервисы по закачке данных в базе ECO (гео-экономика)
# https://w3.unece.org/PXWeb/ru/TableDomains/
# https://www.numbeo.com/cost-of-living/prices_by_country.jsp
# https://restcountries.com/v3.1/all
# https://api.worldbank.org/v2/country/all/indicator/NY.ADJ.NNTY.KD.ZG?format=json&page=1&date=2022&per_page=10
# https://ru.tradingeconomics.com/country-list/personal-income-tax-rate
import socket
import common
import t_countries
import t_worldbank
import t_numbeo
import t_clear_logs
import t_statdata
import t_tradingeconomics
import t_wikipedia
import t_indonesia
import t_spain
import t_georgia
import t_cyprus
import t_courses
import t_meteo
import t_unece
import t_usa
import countries
import un
import bigmac
import time
import config
import tables_csv
import turkey
import russia
import wikipedia
from requests.utils import DEFAULT_CA_BUNDLE_PATH
import geopy
import geocoder

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
    # bigmac.load_bm()  # !!! отдельная песня сделанная Кириллом через эксель и экспорт в свою базу

    # ?????tables_html.load_trading('https://ru.tradingeconomics.com/country-list/full-year-gdp-growth')
    # ?????tables_html.load_trading('https://api.tradingeconomics.com/country/russia')

    # turkey.make_inform()  # отдельная обработка Турции
    # russia.load_inform()
    # turkey.sale_foreign_tourists(25)

    common.write_log_db(
        'START', 'WebParser', 'Старт сервиса подкачки статистической информации;\n' +
        ' version: ' + version +
        '; host: ' + config.URL +
        '; schema: ' + config.SCHEMA,
        file_name=common.get_computer_name()
    )

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

    while True:
        time.sleep(5)
