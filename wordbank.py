import config
import common
import countries as c_countries
import json


def load_data(indicator, param_name, clear_table=False):
    def write_db(st):
        ans, ok, status = common.send_rest('v1/NSI/script/execute', 'PUT', st, lang='en', token_user=token)
        if not ok:
            print(ans)

    # узнать сколько элементов имеется всего
    url = "{url_wordbank}{indicator}?format=json&page={page}&per_page=1".format(
        url_wordbank=config.url_wordbank, indicator=indicator, page=1)
    answer, is_ok, status = common.send_rest_url(url)
    if is_ok:
        answer = json.loads(answer)
        total = answer[0]['total']
        url = "{url_wordbank}{indicator}?format=json&page={page}&per_page={total}".format(
            url_wordbank=config.url_wordbank, indicator=indicator, page=1, total=total)
        answer, is_ok, status = common.send_rest_url(url)  # прочитать все
        if is_ok:
            countries = c_countries.load_countries()
            answer = json.loads(answer)
            token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
            if clear_table:
                # таблицу очистить
                common.send_rest(
                    'v1/NSI/script/execute', 'PUT', "truncate table {schema}.his_countries_{param_name}".format(
                        schema=config.SCHEMA, param_name=param_name), lang='en', token_user=token)
            st_query = ''
            count = 0
            for unit in answer[1]:
                if unit['value']:
                    code = unit['countryiso3code']
                    country_id = None
                    for elem in countries:
                        if elem['code'] == code:
                            country_id = elem['id']
                            break
                    if country_id:  # есть такая страна
                        value = unit['value']
                        year = int(unit['date'])
                        st_query += "select {schema}.pw_his('{param_name}', '{date}-12-01', {country_id}, {value});".\
                            format(param_name=param_name, date=year, country_id=country_id, value=value,
                                   schema=config.SCHEMA)
                        count += 1
                        if count > 100:
                            write_db(st_query)
                            st_query = ''
                            count = 0
            if st_query != '':
                write_db(st_query)

    else:
        print(str(answer))