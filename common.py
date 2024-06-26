import config
import json
import requests
from requests.exceptions import HTTPError
import base64
import time
import datetime
import socket
import re

app_lang = 'ru'
rights = ''


def send_rest(mes, directive="GET", params=None, lang='', token_user=None):
    js = {}
    if token_user is not None:
        js['token'] = token_user
    if lang == '':
        lang = app_lang
    if directive == 'GET' and 'lang=' not in mes:
        if '?' in mes:
            mes = mes + '&lang=' + lang
        else:
            mes = mes + '?lang=' + lang
    else:
        js['lang'] = lang   # код языка пользователя
    if params:
        if type(params) is not str:
            params = json.dumps(params, ensure_ascii=False)
        js['params'] = params  # дополнительно заданные параметры
    try:
        headers = {"Accept": "application/json"}
        response = requests.request(directive, config.URL + mes.replace(' ', '+'), headers=headers, json=js)
    except HTTPError as err:
        txt = f'HTTP error occurred: {err}'
        return txt, False, None
    except Exception as err:
        txt = f'Other error occurred: {err}'
        return txt, False, None
    else:
        return response.text, response.ok, '<' + str(response.status_code) + '> - ' + response.reason


def send_rest_url(mes, directive="GET", show_error=True):
    t = time.time()
    headers = {"Accept": "application/json"}
    try:
        response = requests.request(directive, mes, headers=headers)
    except HTTPError as err:
        txt = f'HTTP error occurred: {err}'
        if show_error:
            print('ERROR', 'Ошибка запроса к URL ' + mes, txt + '; duration= %.3f' % t)
        return txt, False, None
    except Exception as err:
        txt = f'Other error occurred: {err}'
        if show_error:
            print('ERROR', 'Ошибка запроса к URL ' + mes, txt + '; duration= %.3f' % t)
        return txt, False, None
    else:
        t = time.time() - t
        if not response.ok and show_error:
            print('ERROR', response.text, mes + '; duration= %.3f' % t)
        return response.text, response.ok, '<' + str(response.status_code) + '> - ' + response.reason


def decode(key, enc):
    dec = []
    enc = base64.urlsafe_b64decode(enc).decode()
    for i in range(len(enc)):
        key_c = key[i % len(key)]
        dec_c = chr((256 + ord(enc[i]) - ord(key_c)) % 256)
        dec.append(dec_c)
    return "".join(dec)


def encode(key, text):
    enc = []
    for i in range(len(text)):
        key_c = key[i % len(key)]
        enc_c = chr((ord(text[i]) + ord(key_c)) % 256)
        enc.append(enc_c)
    return base64.urlsafe_b64encode("".join(enc).encode()).decode()


def login(e_mail, password):
    global app_lang, rights
    result = False
    txt_z = {"login": e_mail, "password": password, "rememberMe": True, "category": config.SCHEMA}
    try:
        headers = {"Accept": "application/json"}
        response = requests.request(
            'POST', config.URL + 'v1/login', headers=headers,
            json={"params": txt_z}
            )
    except HTTPError as err:
        txt = f'HTTP error occurred: {err}'
    except Exception as err:
        txt = f'Other error occurred: : {err}'
    else:
        try:
            txt = response.text
            result = response.ok
            if result:
                js = json.loads(txt)
                token_db = ''
                if "accessToken" in js:
                    token_db = js["accessToken"]

                token = decode(decode('abcd', config.kirill), token_db)
                token = json.loads(token)
                rights = ''
                for key in token.keys():
                    if key == '***':
                        rights = 'visible, admin'
                    elif token[key] != 'GET' and token[key] != '' and 'visible' in token[key]:
                        rights = token[key]
                        break

                if 'lang' in js:
                    app_lang = js['lang']
                return token_db, result
            else:
                return txt, result
        except Exception as err:
            txt = f'Error occurred: : {err}'
    return txt, result


def st_today():
    return str(time.gmtime().tm_year) + '-' + str(time.gmtime().tm_mon).rjust(2, '0') + '-' + \
           str(time.gmtime().tm_mday).rjust(2, '0')


def st_yesterday():
    dt = datetime.date.today() + datetime.timedelta(days=-1)
    return str(dt.year) + '-' + str(dt.month).rjust(2, '0') + '-' + str(dt.day).rjust(2, '0')


def st_month():
    return str(time.gmtime().tm_year) + '-' + str(time.gmtime().tm_mon).rjust(2, '0') + '-01'


def st_now():
    return str(time.gmtime().tm_year) + '-' + str(time.gmtime().tm_mon).rjust(2, '0') + '-' + \
           str(time.gmtime().tm_mday).rjust(2, '0') + ' ' +\
           str(time.gmtime().tm_hour).rjust(2, '0') + ':' + str(time.gmtime().tm_min).rjust(2, '0') + ':00'


def load_countries(token=None):
    """
    Загрузка списка стран (для определения ID по имени).
    :param token: - токен для RestAPI (для операции PUT) - опционально
    :return: массив стран или None при ошибке чтения
    """
    url = 'v1/select/{schema}/nsi_countries'.format(schema=config.SCHEMA)
    countries, is_ok, status_response = send_rest(url)
    if not is_ok:
        # print(str(countries))
        write_log_db('ERROR', 'load_countries', str(countries) + '; ' + url, token_admin=token)
        return
    countries = json.loads(countries)
    return countries


def get_country_id(name, countries, code=None, pr=True):
    try:
        if code is None:
            name = name.strip()
            name = name.split('[')[0]
            if name == 'USA':
                name = 'United States'
            name = name.replace('<strong>', '').replace('</strong>', '').strip()
            name = name.strip().replace(' (China)', '').replace('Us', 'United States').replace('US', 'United States'). \
                replace('Macao', 'Macau').replace('Lebenon', 'Lebanon').replace('США', 'United States'). \
                replace('Соединенные Штаты', 'United States').replace('United States Америки', 'United States').\
                replace('Сербии', 'Сербия').replace('Шри Ланка', 'Шри-Ланка').\
                replace('Папуа-Новая Гвинея', 'Папуа — Новая Гвинея').replace('ЮАР', 'Южная Африка').\
                replace('Республике Конго', 'Республика Конго').replace('Папуа - Новая Гвинея', 'Папуа — Новая Гвинея')
            if name in ['Саудов. Аравия']:
                name = 'Саудовская Аравия'
            if name in ['Республика Молдова']:
                name = 'Молдавия'
            if name in ['Соединенное Королевство']:
                name = 'Великобритания'
            if name in ['Kosovo (Disputed Territory)', 'Косово']:
                name = 'Республика Косово'
            if name in ['Конго', 'Congo', 'Democratic Republic of Congo', 'ДР Конго']:
                name = 'Демократическая Республика Конго'
            if name in ["Cote d'Ivoire", "Кот-д\'Ивуар", "Берег Слоновой Кости"]:
                name = 'Ivory Coast'
            if name == "Curacao":
                name = 'Curaçao'
            if name == "East Timor":
                name = 'Восточный Тимор'
            if name == "T.R.N.C.":
                name = 'Кипр'
            if name == "U.S.A.":
                name = 'United States'
            if name == "U.A.E." or name == 'ОАЭ':
                name = 'Объединённые Арабские Эмираты'
            if name == "Korea":
                name = 'Южная Корея'
            if name == "Bosnia Herzegovina":
                name = 'Босния и Герцеговина'
            if name == "Kyrghyzstan":
                name = 'Киргизия'
            if name == "Honkong":
                name = 'Гонконг'
            if name == "Bahreyn":
                name = 'Бахрейн'
            if name == "Белоруссия":
                name = 'Беларусь'
            if name == "Кыргызстан":
                name = 'Киргизия'
            if name == "ЦАР":
                name = 'Центральноафриканская Республика'
            if name == "КНДР":
                name = 'Северная Корея'
            if name == "Эсватини":
                name = 'Свазиленд'
            if name == "Македония":
                name = 'Северная Македония'
            if name in ["Коморские Острова", "Коморские острова"]:
                name = 'Коморы'
            if name in ["Эритрее"]:
                name = 'Эритрея'
            if name in ["Микронезия"]:
                name = 'Федеративные Штаты Микронезии'
            if name == 'Brunei Darusalaam':
                name = 'Бруней'
            if name == 'Cocos Islands':
                name = 'Кокосовые острова'
            if name == 'Faeroe Islands':
                name = 'Фарерские острова'
            if name == 'Irish Republic':
                name = 'Ирландия'
            if name == 'Kiribati Republic':
                name = 'Кирибати'
            if name == 'Kirg(h)izia':
                name = 'Киргизия'
            if name == 'Macedonia':
                name = 'Северная Македония'
            if name == 'North Yemen':
                name = 'Йемен'
            if name == 'Reunion':
                name = 'Реюньон'
            if name == 'Rwandese Republic':
                name = 'Руанда'
            if name == 'Swaziland':
                name = 'Свазиленд'
            if name == 'Ta(d)jikistan':
                name = 'Таджикистан'
            if name == 'Zaire':
                name = 'Демократическая Республика Конго'
            for country in countries:
                if name.upper() in [country['sh_name'].upper(), country['official'].upper(), country['name_rus'].upper(),
                                    country['official_rus'].upper()]:
                    return country['id']
            if pr:
                print('absent name', name)
        else:
            for country in countries:
                if code == country['code']:
                    return country['id']
            if pr:
                print('absent code', code, 'name', name)
    except Exception as er:
        print(name, f'{er}')


def get_province_id(name, provinces, code=None, pr=True):
    try:
        if code is None:
            name = name.split('[')[0]
            name = name.replace('<strong>', '').replace('</strong>', '').strip()
            for province in provinces:
                if name.upper() in [province['sh_name'].upper(), province['name_rus'].upper(),
                                    province['name_own'].upper()]:
                    return province['id']
            if pr:
                print('absent name', name)
        else:
            for province in provinces:
                if code == province['code']:
                    return province['id']
            if pr:
                print('absent code', code, 'name', name)
    except Exception as er:
        print(name, f'{er}')


def get_region_id(name, regions, pr=True):
    try:
        name = name.split('[')[0]
        name = name.replace('<strong>', '').replace('</strong>', '').strip()
        for region in regions:
            if region['sh_name'] and name.upper() in [region['sh_name'].upper()] or \
               region['code'] and name.upper() in [region['code'].upper()]:
                return region['id']
        if pr:
            print('absent name', name)
    except Exception as er:
        print(name, f'{er}')


def get_city_id(name_city, cities, pr=True):
    name_city = name_city.split('[')[0]
    name_city = name_city.split('(')[0]
    name_city = name_city.replace('<strong>', '').replace('</strong>', '').strip()
    name_city = name_city.replace('NY', 'New York')
    if name_city == 'Sevilla':
        name_city = 'Seville'
    if name_city == 'Málaga':
        name_city = 'Malaga'
    if name_city == 'Лимасол':
        name_city = 'Лимассол'
    if name_city == 'Тель-Авив-Яффо':
        name_city = 'Тель-Авив'
    name_city = name_city.split('<strong>')
    if len(name_city) != 1:
        name_city = name_city[1]
    else:
        name_city = name_city[0]
    name_city = name_city.replace('NY', 'New York').split('</strong>')[0]

    for city in cities:
        if city['name_own'] is None:
            city['name_own'] = ''
        if name_city.upper() in [city['sh_name'].upper(), city['name_rus'].upper(), city['name_own'].upper()]:
            return city['id']
    if pr:
        print('Absent city', name_city)


def load_cities(token=None):
    url = 'v1/select/{schema}/nsi_cities'.format(schema=config.SCHEMA)
    cities, is_ok, status_response = send_rest(url)
    if not is_ok:
        # print(str(cities))
        write_log_db('ERROR', 'load_cities', str(cities) + '; ' + url, token_admin=token)
        return
    cities = json.loads(cities)
    return cities


def write_script_db(st_query, token=None, write_error=True):
    if st_query:
        if token is None:
            token, is_ok = login('superadmin', decode('abcd', config.kirill))
            if not is_ok:
                write_log_db('ERROR', 'login', str(token))
                print('Error login')
                return False
        answer, ok, status = send_rest(
            'v1/execute', 'PUT', st_query, lang='en', token_user=token)
        if not ok:
            if write_error:
                write_log_db('ERROR', 'v1/execute', str(answer).split(';')[0] + ' ...')
            print(answer)
            return False
        else:
            return True


def write_objects_db(object_code, values, token=None):
    # возвращается None если все хорошо, иначе текст ошибки
    if token is None:
        ans, is_ok, token, lang = login_superadmin()
        if not is_ok:
            print('Error login')
            return str(ans)
    params = {"schema_name": config.SCHEMA, "object_code": object_code, "values": values}
    ans, ok, status_result = send_rest('v1/objects', 'PUT', params=params, token_user=token)
    if not ok:
        print(ans)
        write_log_db('Error', 'write_objects_db', status_result + ': ' + str(ans), file_name=get_computer_name(),
                     token_admin=token)
        return str(ans)


def check_country_name(name_country):
    name_country = name_country.replace('США', 'Соединённые Штаты Америки'). \
        replace('ДР Конго', 'Демократическая Республика Конго').replace('Саудов. Аравия', 'Саудовская Аравия')
    return name_country


def load_from_db(object_code, where=''):
    url = 'v1/select/{schema}/nsi_{object_code}'.format(schema=config.SCHEMA, object_code=object_code)
    if where:
        url += '?where={where}'.format(where=where)
    answer, is_ok, status_response = send_rest(url)
    if not is_ok:
        print(str(answer))
        return []
    return json.loads(answer)


def get_param_name_by_indicator(indicator, source):
    url = "v1/select/{schema}/nsi_import?where=sh_name='{source}' and code='{code}'".format(
        schema=config.SCHEMA, source=source, code=indicator)
    answer, is_ok, status_response = send_rest(
        url, params={"columns": "param_name"})
    if not is_ok:
        print(str(answer))
    else:
        answer = json.loads(answer)
        if len(answer) > 0:
            return answer[0]['param_name']
        else:
            return []


def login_superadmin():
    result = False
    token_admin = ''
    lang_admin = ''
    txt_z = {"login": "superadmin", "password": decode('abcd', config.kirill), "rememberMe": True}
    try:
        headers = {"Accept": "application/json"}
        response = requests.request(
            'POST', config.URL + 'v1/login', headers=headers,
            json={"params": txt_z}
            )
    except HTTPError as err:
        txt = f'HTTP error occurred: {err}'
    except Exception as err:
        txt = f'Other error occurred: : {err}'
    else:
        try:
            txt = response.text
            result = response.ok
            if result:
                js = json.loads(txt)
                if "accessToken" in js:
                    token_admin = js["accessToken"]
                if 'lang' in js:
                    lang_admin = js['lang']
            else:
                token = None
                return txt, result, token, ''
        except Exception as err:
            txt = f'Error occurred: : {err}'
    return txt, result, token_admin, lang_admin


def translate_to_base(st):
    st = st.replace('\n', '~LF~').replace('(', '~A~').replace(')', '~B~').replace('@', '~a1~')
    st = st.replace(',', '~a2~').replace('=', '~a3~').replace('"', '~a4~').replace("'", '~a5~')
    st = st.replace(':', '~a6~').replace('/', '~b1~').replace('&', '~b2~')
    return st


def write_log_db(level, src, msg, page=None, file_name='', law_id='', td=None, write_to_db=True, write_to_consol=True,
                 token_admin=None):
    st_td = '' if td is None else "td=%.1f sec;" % td
    st_file_name = '' if file_name is None or file_name == '' else 'file=' + file_name + ';'
    st_law_id = '' if law_id is None or law_id == '' else 'law_id=' + law_id + ';'
    st_page = '' if page is None or page == '' else 'page=' + str(page) + ';'
    if write_to_consol:
        print( time.asctime(time.gmtime()) + ':', level + ';', src + ';', st_td, st_page, st_law_id,
               st_file_name.replace('\n', ' '), msg.replace('\n', ' '), flush=True)
    if not write_to_db:
        return
    if token_admin is None:
        answer, is_ok, token, lang = login_superadmin()
    else:
        token = token_admin
        is_ok = True
    if is_ok:
        page = 'NULL' if page is None or page == '' else page
        law_id = '' if law_id is None else law_id
        file_name = '' if file_name is None else file_name
        td = 'NULL' if td is None else float("%.1f" % td)
        st = "'{level}', '{source}', '{comment}', {page}, '{law_id}', '{file_name}', {td}".format(
                schema=config.SCHEMA, level=level, source=src,
                comment=translate_to_base(msg.replace("'", '"')),
                page=page, law_id=law_id,
                file_name=translate_to_base(file_name), td=td
              )
        answer, is_ok, status = send_rest(
            'v1/function/{schema}/pw_logs?text={text}'.format(schema=config.SCHEMA, text=st), 'POST',
            token_user=token)
        if not is_ok:
            print(time.ctime(), 'ERROR', 'write_log_db', str(answer), flush=True)
    else:
        print(time.ctime(), 'ERROR', 'write_log_db', 'login_superadmin: ' + str(answer), flush=True)


def get_duration(td):
    result = ''
    if td is None:
        return result
    if '<' in str(td):
        return str(td) + ' sec'
    tdr = int(td + 0.5)
    if tdr == 0:
        return '< 0.5 sec'
    if tdr >= 86400:
        result = str(tdr // 86400) + ' day'
        if tdr // 86400 != 1:
            result = result + 's'
        tdr = tdr % 86400
    result = result + " {hour:02}:{minute:02}:{second:02}".format(
        hour=tdr // 3600, minute=tdr % 3600 // 60, second=tdr % 3600 % 60)
    return result.replace('00:00:00', '')


def get_computer_name():
    st = socket.gethostbyname(socket.gethostname())
    st = '' if st == '127.0.0.1' else st
    return socket.gethostname() + '; ' + st


def define_guest(ip_text, check_simple=True):
    if check_simple and ip_text == '127.0.0.1':
        return '', '', True
    headers = {"Accept": "application/json"}
    try:
        response = requests.request('GET', 'http://ip-api.com/json/{ip}?lang=ru'.format(ip=ip_text), headers=headers)
    except HTTPError as err:
        txt = f'HTTP error occurred: {err}'
        print('ERROR', 'Ошибка запроса', txt)
        return txt, '', False
    try:
        if response.ok:
            answer = json.loads(response.text)
            return answer['country'], answer['city'], True
        else:
            print('ERROR', response.text)
            return response.text.replace('\\n', '\n'), '', False
    except Exception as err:
        return f"{err}", '', False


def get_st_sql(st_sql, name_function, date, id, value, param_name):
    st_sql += "select {schema}.{name_function}('{param_name}', '{date}', {id}, {value});\n". \
        format(param_name=param_name, date=date, id=id, value=value, schema=config.SCHEMA, name_function=name_function)
    return st_sql


def load_provinces_db(country_id):
    url = 'v1/select/{schema}/nsi_provinces?where=country={country_id}'.format(
        schema=config.SCHEMA, country_id=country_id)
    provinces, is_ok, status_response = send_rest(url)
    if not is_ok:
        print(str(provinces))
        return
    provinces = json.loads(provinces)
    return provinces


def time_for_sql(dt, convert=True) -> str:
    if convert:
        dt = dt.toPyDateTime()
    return str(dt.year) + '-' + str(dt.month).zfill(2) + '-' + str(dt.day).zfill(2) + ' ' + \
        str(dt.hour).zfill(2) + ':' + str(dt.minute).zfill(2) + ':' + str(dt.second).zfill(2)


def utc_local(utc: datetime) -> datetime:
    epoch = time.mktime(utc.timetuple())
    offset = datetime.datetime.fromtimestamp(epoch) - datetime.datetime.utcfromtimestamp(epoch)
    return utc + offset


def local_utc(local: datetime) -> datetime:
    """  Перевод local: datetime из локального времени в UTC: datetime """
    epoch = time.mktime(local.timetuple())
    offset = datetime.datetime.fromtimestamp(epoch) - datetime.datetime.utcfromtimestamp(epoch)
    return local - offset


def is_number(s):
    if re.match("^\d+?\.\d+?$", s) is None:
        return s.isdigit()
    return True


def get_name_object(data):
    if data['object_code'] == 'countries':
        return 'страны'
    if data['object_code'] == 'cities':
        return 'города'
    if data['object_code'] == 'provincies':
        return 'провинции (штаты)'
