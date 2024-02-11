"""
Разбор информации из csv файлов
"""
import common
import config
import countries as c_countries

def get_file(filename):
    """
    Прочитать файл с полной информацией сайта
    :return: текст файла или None в случае ошибки
    """
    try:
        f = open(filename, 'r')
        with f:
            return f.readlines()
    except Exception as er:
        print(f'{er}')


def load_inform(filename, param_name, index_value, symbol=',', index_name=0, index_year=1, index_code=None):
    rows = get_file(filename)
    if rows is None:
        return
    countries = c_countries.load_countries()
    if countries is None:
        return
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    if not is_ok:
        return
    rows = rows[1:]  # строку заголовков пропускаем
    st_query = ''
    for row in rows:
        data = row.split(symbol)
        year = data[index_year]
        name = data[index_name]
        value = data[index_value]
        code = None if index_code is None else data[index_code]
        country_id = common.get_country_id(name, countries, code)
        if country_id:
            date = '{year}-01-01'.format(year=year)
            st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});".format(
                date=date, country_id=country_id, value=value, schema=config.SCHEMA, param_name=param_name)
    if st_query:
        answer, ok, status = common.send_rest(
            'v1/NSI/script/execute', 'PUT', st_query, lang='en', token_user=token)
        if not ok:
            print(answer)
