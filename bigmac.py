import config
import common
import json


def load_bm():
    def write_db(st):
        ans, ok, status = common.send_rest('v1/NSI/script/execute', 'PUT', st, lang='en', token_user=token)
        if not ok:
            print(ans)

    def get_st_sql(st_sql, data, key, param_name):
        st_sql += "select {schema}.pw_his('{param_name}', '{date}', {country_id}, {value});". \
            format(param_name=param_name, date=data['date'], country_id=country_id, value=data[key],
                   schema=config.SCHEMA)
        return st_sql

    answer, is_ok, status = common.send_rest(
        'v1/select/kirill/test', params={"columns": "date, iso_a3, local_price, dollar_price, usd_raw"})
    if not is_ok:
        print(str(answer))
        return
    answer = json.loads(answer)
    st_local_price = ''
    st_dollar_price = ''
    st_index_dollar = ''
    countries = common.load_countries()
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    # составим список стран еврозоны
    list_code_euz = []
    st = "select b.code from {schema}.rel_countries_currencies_currencies a, eco.nsi_countries b where " \
         "a.currencies_id = 1 and b.id = a.countries_id;".format(schema=config.SCHEMA)
    ans, is_ok, status = common.send_rest('v1/execute?need_answer=true&view=1', 'PUT', st, lang='en', token_user=token)
    if is_ok:
        ans = json.loads(ans)
        for unit in ans:
            list_code_euz.append(unit['code'])
    count = 0
    for unit in answer:
        if unit['iso_a3'] == 'EUZ':
            list_code = list_code_euz
        else:
            list_code = [unit['iso_a3']]
        for code in list_code:
            country_id = None
            for elem in countries:
                if elem['code'] == code:
                    country_id = elem['id']
                    break
            if country_id:  # есть такая страна
                st_local_price = get_st_sql(st_local_price, unit, 'local_price', 'bm_local_price')
                st_dollar_price = get_st_sql(st_dollar_price, unit, 'dollar_price', 'bm_dollar_price')
                st_index_dollar = get_st_sql(st_index_dollar, unit, 'usd_raw', 'bm_index_dollar')
                count += 1
                if count > 100:
                    write_db(st_local_price)
                    write_db(st_dollar_price)
                    write_db(st_index_dollar)
                    st_local_price = ''
                    st_dollar_price = ''
                    st_index_dollar = ''
                    count = 0
    if st_local_price != '':
        write_db(st_local_price)
        write_db(st_dollar_price)
        write_db(st_index_dollar)
