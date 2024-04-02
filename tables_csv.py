"""
Разбор информации из csv файлов
"""
import common
import config
import time

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


def load_inform(filename, param_name, index_value, symbol=',', index_name=0, index_year=1, index_code=None,
                max_year=None, param_name_nsi=None):
    rows = get_file(filename)
    if rows is None:
        return
    countries = common.load_countries()
    if countries is None:
        return
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    if not is_ok:
        return
    rows = rows[1:]  # строку заголовков пропускаем
    st_query = ''
    nsi = dict()
    for row in rows:
        data = row.split(symbol)
        year = data[index_year]
        if max_year and year >= str(max_year):
            continue
        name = data[index_name]
        value = data[index_value]
        code = None if index_code is None else data[index_code]
        country_id = common.get_country_id(name, countries, code)
        if country_id and value:
            date = '{year}-01-01'.format(year=year)
            st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});\n".format(
                date=date, country_id=country_id, value=value, schema=config.SCHEMA, param_name=param_name)
            nsi[str(country_id)] = value
    common.write_script_db(st_query, token)
    st_nsi = ''
    if param_name_nsi:
        for key in nsi:
            st_nsi += "update {schema}.nsi_countries set {param_name_nsi}={value} where id={country_id};select 1;\n". \
                format(country_id=key, value=nsi[key], schema=config.SCHEMA, param_name_nsi=param_name_nsi)
    common.write_script_db(st_nsi, token)


def get_inform_from_csv():
    load_inform('source/ourworldindata/population_change.csv', 'population_change', index_value=2)  # Общий прирост населения
    load_inform('source/ourworldindata/pops.csv', 'pops', index_value=2)  # Общее население
    load_inform('source/ourworldindata/cost-healthy-diet.csv', 'cost_hd', index_code=1, index_year=2,
                index_value=3)  # Суточная стоимость здорового питания (дол)
    load_inform('source/ourworldindata/human-development-index.csv', 'hdi', index_code=1, index_year=2,
                index_value=3)  # Индекс человеческого развития
    load_inform('source/ourworldindata/air-passengers-carried.csv', 'air_pass_carry', index_code=1,
                index_year=2, index_value=3)  # Количество перевезенных авиапассажиров
    load_inform('source/ourworldindata/tourism-gdp-proportion-of-total-gdp.csv', 'tourism_gdp_per',
                index_code=1, index_year=2, index_value=3)  # Количество перевезенных авиапассажиров
    # Средняя продолжительность пребывания ин. гостей
    load_inform(
      'source/ourworldindata/average-length-of-stay.csv', 'av_day_of_stay',
      index_code=1, index_year=2, index_value=3)
    # Средний возраст населения
    load_inform(
        'source/ourworldindata/median-age.csv', 'av_age', index_code=1, index_year=2, index_value=3,
        max_year=time.gmtime().tm_year, param_name_nsi='av_age_last')


# load_inform('source/ourworldindata/median-age.csv', 'av_age', index_code=1, index_year=2, index_value=3,
#             max_year=time.gmtime().tm_year, param_name_nsi='av_age_last')
