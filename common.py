import config
import json
import requests
from requests.exceptions import HTTPError
import base64
import time
import socket

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


def st_now():
    return str(time.gmtime().tm_year) + '-' + str(time.gmtime().tm_mon).rjust(2, '0') + '-' + \
           str(time.gmtime().tm_mday).rjust(2, '0') + ' ' +\
           str(time.gmtime().tm_hour).rjust(2, '0') + ':' + str(time.gmtime().tm_min).rjust(2, '0') + ':00'


def get_country_id(name, countries, code=None, pr=True):
    try:
        if code is None:
            name = name.strip().replace(' (China)', '').replace('Us', 'United States').replace('US', 'United States'). \
                replace('Macao', 'Macau').replace('Lebenon', 'Lebanon').replace('США', 'United States'). \
                replace('Соединенные Штаты', 'United States').\
                replace('Сербии', 'Сербия').replace('Шри Ланка', 'Шри-Ланка').\
                replace('Папуа-Новая Гвинея', 'Папуа — Новая Гвинея').replace('ЮАР', 'Южная Африка').\
                replace('Республике Конго', 'Республика Конго')
            if name in ['Конго', 'Congo', 'Democratic Republic of Congo', 'ДР Конго']:
                name = 'Республика Конго'
            if name == "Cote d'Ivoire":
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


def get_city_id(name_city, cities):
    name_city = name_city.replace('NY', 'New York')
    name_city = name_city.split('<strong>')
    if len(name_city) != 1:
        name_city = name_city[1]
    else:
        name_city = name_city[0]
    name_city = name_city.replace('NY', 'New York').split('</strong>')[0]

    for city in cities:
        if name_city.upper() in [city['sh_name'].upper(), city['name_rus'].upper()]:
            return city['id']
    print('Absent city', name_city)


def write_script_db(st_query):
    if st_query:
        token, is_ok = login('superadmin', decode('abcd', config.kirill))
        if not is_ok:
            print('Error login')
            return
        answer, ok, status = send_rest(
            'v1/NSI/script/execute', 'PUT', st_query, lang='en', token_user=token)
        if not ok:
            print(answer)


def write_objects_db(object_code, values):
    token, is_ok = login('superadmin', decode('abcd', config.kirill))
    if not is_ok:
        print('Error login')
        return
    params = {"schema_name": config.SCHEMA, "object_code": object_code, "values": values}
    ans, ok, status_result = send_rest('v1/objects', 'PUT', params=params, token_user=token)
    if not ok:
        print(ans)


def check_country_name(name_country):
    name_country = name_country.replace('США', 'Соединённые Штаты Америки'). \
        replace('ДР Конго', 'Демократическая Республика Конго').replace('Саудов. Аравия', 'Саудовская Аравия')
    return name_country


def load_from_db(object_code, columns, where=''):
    url = 'v1/select/{schema}/nsi_{object_code}'.format(schema=config.SCHEMA, object_code=object_code)
    if where:
        url += '?where={where}'.format(where=where)
    answer, is_ok, status_response = send_rest(
        url, params={"columns": columns})
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
                return txt, result, token
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
        print(time.ctime() + ':', level + ';', src + ';', st_td, st_page, st_law_id, st_file_name.replace('\n', ' '),
              msg.replace('\n', ' '), flush=True)
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
    return result


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

