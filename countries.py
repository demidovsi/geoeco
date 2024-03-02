#https://w3.unece.org/PXWeb/ru/TableDomains/
import time
import requests
from requests.adapters import HTTPAdapter
import json

import common
import config

http_adapter = HTTPAdapter(max_retries=10)
api_key = 'DFtDCCAiCZNZIEtAj1aa5VwJzIE8W8Ep'


def str_time(t):
    return '\t%.3f' % t


def receive_inform():
    """
    Получить полную информацию с сайта стран и записать ее в файл result.json в текущую директорию.
    :return: True в случае отсутствия ошибки
    """
    url = 'https://restcountries.com/v3.1/all'
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url, timeout=(100, 100))
    if r.ok:
        answer = json.loads(r.text)
        try:
            f = open('source/result.json', 'w', encoding='utf-8')
            with f:
                f.write(json.dumps(answer, indent=4, ensure_ascii=False, sort_keys=True))
            return True
        except Exception as er:
            print(f'{er}')


def get_inform():
    """
    Прочитать файл с полной информацией сайта
    :return: json в случае отсутствия ошибки или None
    """
    try:
        f = open('source/result.json', 'r', encoding='utf-8')
        with f:
            answer = f.read()
            return json.loads(answer)
    except Exception as er:
        print(f'{er}')


def get_his_text(filename):
    """
    Прочитать файл с полной информацией сайта
    :return: массив строк
    """
    try:
        f = open(filename, 'r', encoding='utf-8')
        with f:
            answer = f.read()
            return answer.split('\n')[1:]
    except Exception as er:
        print(f'{er}')


def load_tld():
    answer = get_inform()  # прочитать файл с полной информацией
    if answer is None:
        return
    tld = list()
    for data in answer:
        if 'tld' not in data:
            continue
        cur = data['tld']
        for key in cur:
            if key not in tld:
                tld.append({"sh_name": key})
    url = 'v1/objects/{schema}/tld'.format(schema=config.SCHEMA)
    answer, is_ok, status_response = common.send_rest(url)  # прочитать существующие суффиксы (для определения ID)
    if not is_ok:
        print(str(answer))
        return
    answer = json.loads(answer)['values']
    for data in answer:
        for unit in tld:
            if unit['sh_name'] == data['sh_name']:
                unit['id'] = data['id']  # этот суффикс будет корректироваться (на всякий случай)
                break
    common.write_objects_db('tld', tld)


def load_languages():
    answer = get_inform()  # прочитать файл с полной информацией
    if answer is None:
        return
    languages = list()
    for data in answer:
        if 'languages' not in data:
            continue
        cur = data['languages']
        for key in cur.keys():
            exist = False
            for unit in languages:
                if unit['code'] == key:
                    exist = True
                    break
            if not exist:
                languages.append({"code": key, "sh_name": cur[key]})
    url = 'v1/objects/{schema}/languages'.format(schema=config.SCHEMA)
    answer, is_ok, status_response = common.send_rest(url)  # прочитать существующие валюты (для определения ID)
    if not is_ok:
        print(str(answer))
        return
    answer = json.loads(answer)['values']
    for data in answer:
        for unit in languages:
            if unit['code'] == data['code']:
                unit['id'] = data['id']  # эта валюта будет корректироваться (на всякий случай)
                break
    common.write_objects_db('languages', languages)


def load_currencies():
    answer = get_inform()  # прочитать файл с полной информацией
    if answer is None:
        return
    currencies = list()
    for data in answer:
        if 'currencies' not in data:
            continue
        cur = data['currencies']
        for key in cur.keys():
            exist = False
            for unit in currencies:
                if unit['code'] == key:
                    exist = True
                    break
            if not exist:
                currencies.append({"code": key, "sh_name": cur[key]['name'],
                                   "symbol": cur[key]['symbol'] if 'symbol' in cur[key] else ''})
    url = 'v1/objects/{schema}/currencies'.format(schema=config.SCHEMA)
    answer, is_ok, status_response = common.send_rest(url)  # прочитать существующие валюты (для определения ID)
    if not is_ok:
        print(str(answer))
        return
    answer = json.loads(answer)['values']
    for data in answer:
        for unit in currencies:
            if unit['code'] == data['code']:
                unit['id'] = data['id']  # эта валюта будет корректироваться (на всякий случай)
                break
    common.write_objects_db('currencies', currencies)
    url = 'v1/objects/{schema}/currencies'.format(schema=config.SCHEMA)
    answer, is_ok, status_response = common.send_rest(url)  # прочитать валюты с ID
    if not is_ok:
        print(str(answer))
        return
    answer = json.loads(answer)['values']
    params = list()
    for data in answer:
        params.append({'schema_name': config.SCHEMA, "object_code": "currencies", "param_code": "course",
                       "obj_id": data['id'], 'discret_sec': 86400, "type_his": "data"})
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    answer, is_ok, status_response = common.send_rest(
        'v1/MDM/his/link', directive='PUT', params=params, token_user=token)
    if not is_ok:
        print(str(answer))


def load_list_countries():
    answer = get_inform()  # прочитать файл с полной информацией
    if answer is None:
        return
    list_countries = list()
    for data in answer:
        param = dict()
        if 'cca3' not in data or 'name' not in data:
            continue
        param['code'] = data['cca3']
        param['sh_name'] = data['name']['official']
        list_countries.append(param)
    url = 'v1/objects/{schema}/list_countries'.format(schema=config.SCHEMA)
    answer, is_ok, status_response = common.send_rest(url)  # прочитать существующие страны (для определения ID)
    if not is_ok:
        print(str(answer))
        return
    answer = json.loads(answer)['values']
    for data in answer:
        for unit in list_countries:
            if unit['code'] == data['code']:
                unit['id'] = data['id']  # эта страна будет корректироваться (на всякий случай)
                break
    common.write_objects_db('list_countries', list_countries)


def load_courses(st_date):
    url = 'v1/select/{schema}/nsi_currencies'.format(schema=config.SCHEMA)
    currencies, is_ok, status_response = common.send_rest(url, params={"columns": "id, code"})
    if not is_ok:
        print(str(currencies))
        return
    currencies = json.loads(currencies)
    list_currencies = ''
    for data in currencies:
        list_currencies = list_currencies + ',' if list_currencies else list_currencies
        list_currencies = list_currencies + data['code']

    url = "https://api.apilayer.com/exchangerates_data/" + st_date + "?base=USD"
    headers = {"apikey": api_key}
    # прочитать валюту с сайта
    response = requests.request('GET', url + "&symbols=" + list_currencies, headers=headers, timeout=300)
    if response.ok:
        status = json.loads(response.text)
        result = status['rates']
        st_dt = status['date']
        st_query = ''
        for unit in result:
            value = result[unit]
            st_query = st_query + '\n' if st_query != '' else st_query
            currency_id = None
            for data in currencies:
                if data['code'] == unit:
                    currency_id = data['id']
                    break
            if currency_id:
                st_query = st_query + "select {schema}.pw_his_currencies('{date}', '{currency_id}', {value});".format(
                    date=st_dt, currency_id=currency_id, value=1 / value, schema=config.SCHEMA
                )
        common.write_script_db(st_query)
    else:
        print(response.status_code, response.text)


def define_id_countries(countries):
    # определить id стран в словаре countries
    url = 'v1/select/{schema}/nsi_countries'.format(schema=config.SCHEMA)
    answer, is_ok, status_response = common.send_rest(url, params={"columns": "official, id"})
    if not is_ok:
        print(str(answer))
        return False
    answer = json.loads(answer)
    for data in answer:
        for unit in countries:
            if unit['official'] == data['official']:
                unit['id'] = data['id']  # эта страна будет корректироваться (на всякий случай)
                break
    return True


def read_entity(name_entity):
    # прочитать существующие континенты (для определения ID)
    url = 'v1/objects/{schema}/{name_entity}'.format(schema=config.SCHEMA, name_entity=name_entity)
    answer, is_ok, status_response = common.send_rest(url)
    if not is_ok:
        print(status_response, str(answer))
        return
    return json.loads(answer)['values']


def set_relation(object_code, code_rel, countries, relations, key, token, name_code='code'):
    # теперь удалить все привязки к языкам
    txt = 'v1/execute'
    script = "delete from {schema}.rel_{object_code}_{code_rel}_{code_rel}; select 'ok';".format(
        schema=config.SCHEMA, object_code=object_code, code_rel=code_rel)
    answer, is_ok, status_response = common.send_rest(txt, 'PUT', params=script, token_user=token)
    if not is_ok:
        print(str(answer))
        return
    # теперь надо записать языки
    params = list()
    for data in countries:
        if key in data:
            for unit in relations:
                param = dict()
                param['obj_id'] = unit['id']
                if name_code in unit:
                    code = unit[name_code]
                    if code in data[key]:
                        param['value'] = 1
                        if 'id' in data:
                            param['id'] = data['id']
                            params.append(param)
    if len(params) > 0:
        txt = 'v1/relations/{schema}/{object_code}/{code_rel}'.format(
            schema=config.SCHEMA, object_code=object_code, code_rel=code_rel)
        answer, is_ok, status_response = common.send_rest(txt, 'PUT', params=params, token_user=token)
        if not is_ok:
            print(str(answer))
            return
    return True


def prepare_countries(answer, continents):
    result = list()
    for data in answer:
        if 'name' not in data:
            continue
        cur = data['name']
        name_rus = ''
        official_rus = ''
        if 'translations' in data:
            for key in data['translations'].keys():
                if key == 'rus':
                    name_rus = data['translations'][key]['common']
                    official_rus = data['translations'][key]['official']
                    break
        params = {"sh_name": cur['common'], "official": cur['official'], "name_rus": name_rus,
                  "official_rus": official_rus, "code": data["cca3"] if "cca3" in data else '',
                  "landlocked": data["landlocked"],
                  "start_of_week": data["startOfWeek"], "status": data["status"]}
        if "independent" in data:
            params["independent"] = data['independent']
        if "unMember" in data:
            params["un_member"] = data['unMember']
        if 'area' in data:
            params['area'] = data['area']
        if 'population' in data:
            params['population'] = data['population']
        if 'car' in data and 'side' in data['car']:
            params['car_side'] = data['car']['side']
        if 'coatOfArms' in data and 'svg' in data['coatOfArms']:
            params['coat_of_arms'] = data['coatOfArms']['svg']
        if 'flags' in data:
            if 'alt' in data['flags']:
                params['flag_alt'] = data['flags']['alt']
            if 'svg' in data['flags']:
                params['flag_svg'] = data['flags']['svg']
        if 'capital' in data:
            st = ''
            array = data['capital']
            for ar in array:
                st = st + ', ' if st else ''
                st += ar
            params['capital'] = st
        if 'timezones' in data:
            st = ''
            array = data['timezones']
            for ar in array:
                st = st + ', ' if st else ''
                st += ar
            params['timezones'] = st
        if 'latlng' in data:
            params['capital_lat'] = data['latlng'][0]
            params['capital_lon'] = data['latlng'][1]
        if 'region' in data:
            region = data['region']
            for unit in continents:
                if 'code' in unit and unit['code'] == region:
                    params['region'] = unit['id']
                    break
        # запомним массив кодов континентов
        if 'continents' in data:
            params['relation_continents'] = data['continents']
        # запомним массив кодов языков
        if 'languages' in data:
            array = list()
            for ar in data['languages'].keys():
                array.append(ar)
            params['relation_languages'] = array
        # запомним массив кодов валют
        if 'currencies' in data:
            array = list()
            for ar in data['currencies'].keys():
                array.append(ar)
            params['relation_currencies'] = array
        # запомним массив суффиксов
        if 'tld' in data:
            params['relation_tld'] = data['tld']
        # запомним массив границ
        if 'borders' in data:
            params['relation_borders'] = data['borders']
        result.append(params)
    return result


def make_countries():
    t1 = time.time()
    t0 = time.time()
    answer = get_inform()  # прочитать файл с полной информацией
    if answer is None:
        return

    # прочитать существующие континенты (для определения ID) Relation
    continents = read_entity('continent')
    print(str_time(time.time() - t0), "\tread_entity('continent')", str_time(time.time() - t1), flush=True)
    t0 = time.time()
    if continents is None:
        return

    # прочитать существующий список стран (для определения ID) Relation
    list_countries = read_entity('list_countries')
    print(str_time(time.time() - t0), "\tread_entity('list_countries')", str_time(time.time() - t1), flush=True)
    t0 = time.time()
    if list_countries is None:
        return

    # прочитать существующие языки (для определения ID) Relation
    languages = read_entity('languages')
    print(str_time(time.time() - t0), "\tread_entity('languages')", str_time(time.time() - t1), flush=True)
    t0 = time.time()
    if languages is None:
        return

    # прочитать существующие валюты (для определения ID) Relation
    currencies = read_entity('currencies')
    print(str_time(time.time() - t0), "\tread_entity('currencies')", str_time(time.time() - t1), flush=True)
    t0 = time.time()
    if currencies is None:
        return

    # прочитать существующие суффиксы (для определения ID) Relation
    tld = read_entity('tld')
    print(str_time(time.time() - t0), "\tread_entity('tld')", str_time(time.time() - t1), flush=True)
    t0 = time.time()
    if tld is None:
        return

    countries = prepare_countries(answer, continents)
    print(str_time(time.time() - t0), "\tprepare countries", str_time(time.time() - t1), flush=True)
    t0 = time.time()

    # определить ID уже существующих стран
    if not define_id_countries(countries):
        return
    print(str_time(time.time() - t0), "\tdefine_id_countries(countries)", str_time(time.time() - t1), flush=True)
    t0 = time.time()

    # записать информацию по странам
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    print(str_time(time.time() - t0), "\t\tlogin", str_time(time.time() - t1), flush=True)
    t0 = time.time()

    if is_ok:
        common.write_objects_db('countries', countries)
        print(str_time(time.time() - t0), "\tPUT countries", str_time(time.time() - t1), flush=True)
        # теперь надо записать континенты
        t0 = time.time()
        set_relation('countries', 'continent', countries, continents, 'relation_continents', token)
        print(str_time(time.time() - t0), "\tset_relation continents", str_time(time.time() - t1), flush=True)

        # теперь записать привязки к языкам
        t0 = time.time()
        set_relation('countries', 'languages', countries, languages, 'relation_languages', token)
        print(str_time(time.time() - t0), "\tset_relation languages", str_time(time.time() - t1), flush=True)

        # теперь записать привязки к валютам
        t0 = time.time()
        set_relation('countries', 'currencies', countries, currencies, 'relation_currencies', token)
        print(str_time(time.time() - t0), "\tset_relation currencies", str_time(time.time() - t1), flush=True)

        # теперь записать привязки к суффиксам
        t0 = time.time()
        set_relation('countries', 'tld', countries, tld, 'relation_tld', token, name_code='sh_name')
        print(str_time(time.time() - t0), "\tset_relation tld", str_time(time.time() - t1), flush=True)

        # теперь записать привязки к границам
        t0 = time.time()
        set_relation('countries', 'list_countries', countries, list_countries, 'relation_borders', token)
        print(str_time(time.time() - t0), "\tset_relation borders", str_time(time.time() - t1), flush=True)
    else:
        print('error', 'login')


def load_countries():
    url = 'v1/select/{schema}/nsi_countries'.format(schema=config.SCHEMA)
    countries, is_ok, status_response = common.send_rest(
        url, params={"columns": "id, code, name_rus, official_rus, sh_name, official"})
    if not is_ok:
        print(str(countries))
        return
    countries = json.loads(countries)
    return countries
