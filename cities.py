import common
import config
import countries as c_countries
import json
import time
from deep_translator import GoogleTranslator
import requests
from requests.adapters import HTTPAdapter

http_adapter = HTTPAdapter(max_retries=10)


def get_his_text(filename):
    """
    Прочитать файл с информацией
    :return: массив строк
    """
    try:
        f = open(filename, 'r', encoding='utf-8')
        with f:
            answer = f.read()
            return answer.split('\n')
    except Exception as er:
        print(f'{er}')


def load_cities():
    url = 'v1/select/{schema}/nsi_cities'.format(schema=config.SCHEMA)
    cities, is_ok, status_response = common.send_rest(
        url, params={"columns": "id, name_rus, sh_name, population, square, country"})
    if not is_ok:
        print(str(cities))
        return
    cities = json.loads(cities)
    return cities


def get_st_sql(st_sql, date, city_id, value, param_name):
    st_sql += "select {schema}.pw_his_cities('{param_name}', '{date}', {city_id}, {value});". \
        format(param_name=param_name, date=date, city_id=city_id, value=value,
               schema=config.SCHEMA)
    return st_sql


def make_form_government(filename):
    countries = c_countries.load_countries()
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    if not is_ok:
        return
    if countries is None:
        return
    answer = get_his_text(filename)  # прочитать файл с информацией в .txt
    if answer is None:
        return
    values = list()
    for data in answer:
        if data.strip() == '':
            continue
        unit = data.strip().split(';')
        name_country = unit[1]
        name_government = unit[2]
        country_id = None
        for country in countries:
            if name_country == country['name_rus'] or name_country == country['sh_name'] or \
                    name_country == country['official_rus']:
                country_id = country['id']
                params = dict()
                params['id'] = country_id
                params['government'] = name_government
                values.append(params)
                break
        if country_id is None:
            print(name_country)

    params = {"schema_name": config.SCHEMA, "object_code": "countries", "values": values}
    ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=token)
    if not ok:
        print(ans)


def make_type_government(filename):
    countries = c_countries.load_countries()
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    if not is_ok:
        return
    if countries is None:
        return
    answer = get_his_text(filename)  # прочитать файл с информацией в .txt
    if answer is None:
        return
    values = list()
    for data in answer:
        if data.strip() == '':
            continue
        unit = data.strip().split(';')
        name_country = unit[1]
        name_type = unit[2]
        country_id = None
        for country in countries:
            if name_country == country['name_rus'] or name_country == country['sh_name'] or \
                    name_country == country['official_rus']:
                country_id = country['id']
                params = dict()
                params['id'] = country_id
                params['type_government'] = name_type
                values.append(params)
                break
        if country_id is None:
            print(name_country)

    params = {"schema_name": config.SCHEMA, "object_code": "countries", "values": values}
    ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=token)
    if not ok:
        print(ans)


def load_cities_html(url):
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url, timeout=(100, 100))
    if r.ok:
        countries = c_countries.load_countries()
        cities = load_cities()
        values = list()
        lws = r.text.split('<figure class="wp-block-table is-style-stripes">')[1].split('tbody>')[1].\
            split('</table>')[0].split('<tr>')[2:-1]
        # сформируем или скорректируем список крупнейших городов мира
        for i, data in enumerate(lws):
            data = data.replace('</td>', ';').replace('<td>', '').replace('</tr>', '').replace('N / A', '')
            lws[i] = data
            unit = data.strip().split(';')
            name_city = unit[1]
            name_country = unit[2].replace('США', 'Соединённые Штаты Америки').\
                replace('ДР Конго', 'Демократическая Республика Конго').replace('Саудов. Аравия', 'Саудовская Аравия')
            population_2018 = unit[3].replace(' ', '')
            population = None
            area = None
            if len(unit) >= 5:
                population = unit[4].replace(' ', '')
            if len(unit) >= 6:
                area = unit[5].replace(' ', '')
            # найти id страны
            country_id = None
            for country in countries:
                if name_country == country['name_rus'] or name_country == country['sh_name']:
                    country_id = country['id']
                    params = dict()
                    params["name_rus"] = name_city
                    params["country"] = country_id
                    params['population'] = population if population else population_2018
                    params['sh_name'] = GoogleTranslator(source='ru', target='en').translate(name_city)
                    if area:
                        params['area'] = area
                    for city in cities:
                        if name_city == city['name_rus'] or name_city == city['sh_name']:
                            params['id'] = city['id']
                            break
                    values.append(params)
                    break
            if country_id is None:
                print(name_country)

        token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
        params = {"schema_name": config.SCHEMA, "object_code": "cities", "values": values}
        ans, ok, status_result = common.send_rest('v1/objects', 'PUT', params=params, token_user=token)
        if not ok:
            print(ans)
        # прочитать новый список городов
        cities = load_cities()
        st_sql = ''
        date = str(time.gmtime().tm_year) + '-' + str(time.gmtime().tm_mon).rjust(2, '0') + '-' + \
               str(time.gmtime().tm_mday).rjust(2, '0')

        for data in lws:
            if data.strip() == '':
                continue
            unit = data.strip().split(';')
            name_city = unit[1]
            population_2018 = unit[3].replace(' ', '')
            population = None
            area = None
            if len(unit) >= 5:
                population = unit[4].replace(' ', '')
            if len(unit) >= 6:
                area = unit[5].replace(' ', '')
            for city in cities:
                if name_city == city['name_rus'] or name_city == city['sh_name']:
                    city_id = city['id']
                    if population:
                        st_sql = get_st_sql(st_sql, date, city_id, population, 'pops')
                        st_sql = get_st_sql(st_sql, '2018-12-01', city_id, population_2018, 'pops')
                    if area:
                        st_sql = get_st_sql(st_sql, date, city_id, area, 'area')
                    break
        if st_sql:
            ans, ok, status = common.send_rest('v1/NSI/script/execute', 'PUT', st_sql, lang='en', token_user=token)
            if not ok:
                print(ans)
