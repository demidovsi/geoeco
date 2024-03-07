import time
import common
import config
import cities as c_cities
import countries as c_countries
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from deep_translator import GoogleTranslator
import json
import trafaret_thread

http_adapter = HTTPAdapter(max_retries=10)
obj = None


def make_numbeo_crime(lws, year, param_name, count, i_name, i_value):
    # годовая информация
    """
    :param lws: массив колонок строк таблицы (без шапки)
    :param year: - год информации
    :param param_name: - имя параметра в БД
    :param count: - количество колонок в строке таблицы (без номера строки)
    :param i_name: - индекс колонки с именем страны в строке таблицы
    :param i_value:  - индекс колонки со значением в строке таблицы
    :return:
    """
    countries = c_countries.load_countries()
    if countries is None:
        return
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    if not is_ok:
        return
    date = '{year}-01-01'.format(year=year)
    print(date, len(lws))
    st_query = ''
    for i in range(0, len(lws), count):
        name = lws[i + i_name]
        value = lws[i + i_value]
        country_id = None
        name = name.replace(' (China)', '').replace('Us', 'United States')
        for country in countries:
            if name.upper() in [country['sh_name'].upper(), country['official'].upper()]:
                country_id = country['id']
                break
        if country_id is None:
            print('absent', name)
            continue
        if value:
            st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});".format(
                date=date, country_id=country_id, value=value, schema=config.SCHEMA, param_name=param_name)
    common.write_script_db(st_query)


def make_numbeo_table(lws, year, param_name, count, i_name, i_value):
    # годовые значения
    """
    :param lws: массив колонок строк таблицы (без шапки)
    :param year: - год информации
    :param param_name: - имя параметра в БД
    :param count: - количество колонок в строке таблицы (без номера строки)
    :param i_name: - индекс колонки с именем страны в строке таблицы
    :param i_value:  - индекс колонки со значением в строке таблицы
    :return:
    """
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    if not is_ok:
        print('Error login')
        return
    countries = c_countries.load_countries()
    if countries is None:
        return
    cities = c_cities.load_cities()
    if cities is None:
        return
    date = '{year}-01-01'.format(year=year)
    print(date, len(lws))
    st_sql = ''
    need_reload = False
    # проверим и запишем новые города
    for i in range(0, len(lws), count):
        name = lws[i + i_name].split(',')
        if len(name) == 2:
            name_country = name[1].strip()
        else:
            name_country = name[2].strip()
        name_city = name[0].strip()
        city_id = common.get_city_id(name_city, cities)
        if city_id is None:
            country_id = common.get_country_id(name_country, countries)
            if country_id:
                params = dict()
                params["country"] = country_id
                params['sh_name'] = name_city
                params['name_rus'] = GoogleTranslator(source='en', target='ru').translate(name_city)
                # записать новый город
                params = {"schema_name": config.SCHEMA, "object_code": "cities", "values": params}
                ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=token)
                if not ok:
                    print(ans)
                else:
                    need_reload = True
    if need_reload:
        cities = c_cities.load_cities()  # прочитать новый список
    for i in range(0, len(lws), count):
        name = lws[i + i_name].split(',')
        name_city = name[0].strip()
        value = lws[i + i_value]
        city_id = common.get_city_id(name_city, cities)
        if city_id and value and value != '-':
            st_sql = get_st_sql_cities(st_sql, date, city_id, value, param_name)
    # записать исторические данные
    common.write_script_db(st_sql)


def get_st_sql_cities(st_sql, date, city_id, value, param_name):
    st_sql += "select {schema}.pw_his_cities('{param_name}', '{date}', {city_id}, {value});\n". \
        format(param_name=param_name, date=date, city_id=city_id, value=value,
               schema=config.SCHEMA)
    return st_sql


def load_inform_countries():
    for year in range(2012, time.gmtime().tm_year + 1):
        lws = load_html(
            "https://www.numbeo.com/crime/rankings_by_country.jsp?title={year}".format(year=year))
        if lws:
            make_numbeo_crime(lws, year, 'crime', 3, 0, 1)  # индекс преступности

    for year in range(2012, time.gmtime().tm_year + 1):
        lws = load_html(
            "https://www.numbeo.com/quality-of-life/rankings_by_country.jsp?title={year}".format(year=year))
        if lws:
            make_numbeo_crime(lws, year, 'life_index', 10, 0, 1)  # Индекс качества жизни
            make_numbeo_crime(lws, year, 'ability_index', 10, 0, 2)  # Индекс покупательной способности
            make_numbeo_crime(lws, year, 'safety_index', 10, 0, 3)  # Индекс безопасности (криминал)
            make_numbeo_crime(lws, year, 'health_index', 10, 0, 4)  # Индекс здравоохранения
            make_numbeo_crime(lws, year, 'cost_index', 10, 0, 5)  # Индекс стоимости жизни (% к Нью-Йорку)
            make_numbeo_crime(lws, year, 'pp_to_income_ratio', 10, 0, 6)  # Соотношение цены дома к доходу
            make_numbeo_crime(lws, year, 'trafic_time_index', 10, 0, 7)  # Индекс времени в пути на дорогах
            make_numbeo_crime(lws, year, 'pollution_index', 10, 0, 8)  # Индекс загрязнения
            make_numbeo_crime(lws, year, 'climate_index', 10, 0, 9)  # Индекс климата

    for year in range(2012, time.gmtime().tm_year + 1):
        lws = load_html(
            "https://www.numbeo.com/property-investment/rankings_by_country.jsp?title={year}".format(year=year))
        if lws:
            make_numbeo_crime(lws, year, 'price_to_income', 8, 0, 1)  # цена дома к доходу (отношение)
            make_numbeo_crime(lws, year, 'gr_yield_cc', 8, 0, 2)  # валовый доход от аренды в центре города
            make_numbeo_crime(lws, year, 'gr_yield_co', 8, 0, 3)  # Валовый доход от аренды вне центра города
            make_numbeo_crime(lws, year, 'price_to_rent_cc', 8, 0, 4)  # Прогноз окупаемости в центре (лет)
            make_numbeo_crime(lws, year, 'price_to_rent_co', 8, 0, 5)  # Прогноз окупаемости не в центре (лет)
            make_numbeo_crime(lws, year, 'mortgage_per_income', 8, 0, 6)  # Часть дохода для погашения ипотеки %
            make_numbeo_crime(lws, year, 'affordability_index', 8, 0, 7)  # Индекс доступности

    # make_numbeo_salary(105, 'av_salary_net')  # Среднемесячная зарплата (дол) после налогов
    # make_numbeo_salary(106, 'mortage_per')  # Процентная ставка по ипотеке % на 20 лет (города)
    # make_numbeo_salary(101, 'price_sm_aoc')  # Цена квадратного метра жилья не в центре города
    # make_numbeo_salary(100, 'price_sm_acc')  # Цена квадратного метра жилья в центре города
    # make_numbeo_salary(11, 'eggs')  # Цена 12 яиц (регуляр)
    # make_numbeo_salary(7, 'water')  # Цена 0.33 литра воды (ресторан)
    # make_numbeo_salary(13, 'water15')  # Цена 1.5 литра воды (регуляр)

def load_html(url):
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url, timeout=(100, 100))
    if r.ok:
        lws = BeautifulSoup(r.text, 'html.parser').\
            find_all('table', class_='stripe row-border order-column compact')[0].\
            find_all('tbody')[0].text.split('\n')
        i = len(lws) - 1
        while i >= 0:
            if lws[i].strip() == '':
                lws.pop(i)
            i -= 1
        return lws


def load_inform_cities():
    for year in range(2012, time.gmtime().tm_year + 1):
        lws = load_html(
            "https://www.numbeo.com/quality-of-life/rankings.jsp?title={year}".format(year=year))
        if lws:
            make_numbeo_table(lws, year, 'life_index', 10, 0, 1)  # Индекс качества жизни
            make_numbeo_table(lws, year, 'ability_index', 10, 0, 2)  # Индекс покупательной способности
            make_numbeo_table(lws, year, 'safety_index', 10, 0, 3)  # Индекс безопасности (криминал)
            make_numbeo_table(lws, year, 'health_index', 10, 0, 4)  # Индекс здравоохранения
            make_numbeo_table(lws, year, 'cost_index', 10, 0, 5)  # Индекс стоимости жизни (% к Нью-Йорку)
            make_numbeo_table(lws, year, 'pp_to_income_ratio', 10, 0, 6)  # Соотношение цены дома к доходу
            make_numbeo_table(lws, year, 'trafic_time_index', 10, 0, 7)  # Индекс времени в пути на дорогах
            make_numbeo_table(lws, year, 'pollution_index', 10, 0, 8)  # Индекс загрязнения
            make_numbeo_table(lws, year, 'climate_index', 10, 0, 9)  # Индекс климата

    for year in range(2012, time.gmtime().tm_year + 1):
        lws = load_html(
            "https://www.numbeo.com/property-investment/rankings.jsp?title={year}".format(year=year))
        if lws:
            make_numbeo_table(lws, year, 'price_to_income', 8, 0, 1)  # Цена дома к доходу (отношение)
            make_numbeo_table(lws, year, 'gr_yield_cc', 8, 0, 2)  # Валовый доход от аренды в центре города %
            make_numbeo_table(lws, year, 'gr_yield_co', 8, 0, 3)  # Валовый доход от аренды вне центра города %
            make_numbeo_table(lws, year, 'price_to_rent_cc', 8, 0, 4)  # Прогноз окупаемости в центре (лет)
            make_numbeo_table(lws, year, 'price_to_rent_co', 8, 0, 5)  # Прогноз окупаемости не в центре (лет)
            make_numbeo_table(lws, year, 'mortgage_per_income', 8, 0, 6)  # Часть дохода для погашения ипотеки %
            make_numbeo_table(lws, year, 'affordability_index', 8, 0, 7)  # Индекс доступности

    for year in range(2012, time.gmtime().tm_year + 1):
        lws = load_html(
            "https://www.numbeo.com/crime/rankings.jsp?title={year}".format(year=year))
        if lws:
            make_numbeo_table(lws, year, 'crime_index', 3, 0, 1)  # Индекс криминала


def get_his_numbeo(filename):
    """
    Прочитать файл с информацией по воде и т.п.
    (view-source:https://www.numbeo.com/cost-of-living/prices_by_country.jsp?displayCurrency=USD&itemId=13)
    :return: массив строк
    """
    try:
        f = open(filename, 'r', encoding='utf-8')
        with f:
            answer = f.read()
            return json.loads(answer)
    except Exception as er:
        print(f'{er}')


def make_history_numbeo(filename, param_name):
    countries = c_countries.load_countries()
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    if not is_ok:
        return
    if countries is None:
        return
    answer = get_his_numbeo(filename)  # прочитать файл с водой
    if answer is None:
        return
    date = answer['date']
    answer = answer['data']
    st_query = ''
    for key in answer:
        country_id = None
        st = key.replace(' (China)', '')
        for country in countries:
            if st.upper() in [country['sh_name'].upper(), country['official'].upper()]:
                country_id = country['id']
                break
        if country_id is None:
            print(key)
            continue
        value = answer[key]
        if value:
            st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});".format(
                date=date, country_id=country_id, value=value, schema=config.SCHEMA, param_name=param_name)
    answer, ok, status = common.send_rest(
        'v1/NSI/script/execute', 'PUT', st_query, lang='en', token_user=token)
    if not ok:
        print(status)
