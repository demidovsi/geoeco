import config
import json
import requests
from requests.exceptions import HTTPError
import base64
import time

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
