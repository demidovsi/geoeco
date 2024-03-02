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


def make_type_government(filename):
    countries = c_countries.load_countries()
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
    common.write_objects_db('countries', values)


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
            name_city = unit[1].replace('<strong>', '').replace('</strong>', '')
            name_country = common.check_country_name(unit[2])
            population_2018 = unit[3].replace(' ', '')
            population = None
            area = None
            if len(unit) >= 5:
                population = unit[4].replace(' ', '')
            if len(unit) >= 6:
                area = unit[5].replace(' ', '')
            # найти id страны
            country_id = common.get_country_id(name_country, countries)
            if country_id:
                params = dict()
                params["name_rus"] = name_city
                params["country"] = country_id
                params['population'] = population if population else population_2018
                params['sh_name'] = GoogleTranslator(source='ru', target='en').translate(name_city)
                if area:
                    params['square'] = area
                city_id = common.get_city_id(name_city, cities)
                if city_id:
                    params['id'] = city_id
                values.append(params)
            else:
                print(name_country)

        common.write_objects_db('cities', values)
        # прочитать новый список городов
        cities = load_cities()
        st_sql = ''
        date = str(time.gmtime().tm_year) + '-' + str(time.gmtime().tm_mon).rjust(2, '0') + '-01'

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
            city_id = common.get_city_id(name_city, cities)
            if city_id:
                if population:
                    st_sql = get_st_sql(st_sql, date, city_id, population, 'pops')
                elif population_2018:
                    st_sql = get_st_sql(st_sql, '2018-12-01', city_id, population_2018, 'pops')
                if area:
                    st_sql = get_st_sql(st_sql, date, city_id, area, 'area')
        common.write_script_db(st_sql)
