import common
import config
import countries as c_countries
import json


def get_his_population(filename):
    """
    Прочитать файл с полной информацией сайта
    :return: json в случае отсутствия ошибки или None
    """
    try:
        f = open(filename, 'r', encoding='utf-8')
        with f:
            answer = f.read()
            return json.loads(answer)
    except Exception as er:
        print(f'{er}')


def make_history(filename, param_name):
    countries = c_countries.load_countries()
    if countries is None:
        return
    answer = get_his_population(filename)  # прочитать файл с информацией (https://w3.unece.org/PXWeb/ru/Table?IndicatorCode=25)
    if answer is None:
        return
    for data in answer['DataTable']:
        code = data['Country']['Alpha3Code']
        years = data['Periods']
        values = data['Values']
        country_id = None
        for country in countries:
            if country['code'] == code:
                country_id = country['id']
                break
        if country_id is None:
            continue
        st_query = ''
        for i, year in enumerate(years):
            st_dt = "{year}-01-01".format(year=year)
            value = values[i]
            if value:
                st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});".format(
                    param_name=param_name, date=st_dt, country_id=country_id, value=value, schema=config.SCHEMA
            )
        common.write_script_db(st_query)


def load_inform():
    make_history('source/UN/Доля торговли, гост.услуг, транспорта и связи в ВВП (gdp_market).json', 'gdp_market')  # Доля торговли, гост.услуг, транспорта и связи в ВВП (%)
    make_history('source/UN/Доля строительства в ВВП (gdp_building).json', 'gdp_building')  # Доля строительства в ВВП (%)
    make_history('source/UN/GDPP.json', 'gdpp')  # ВВП(ППС) (дол)
    make_history('source/UN/gdp_import.json', 'gdp_import')  # Доля импорта в ВВП %
    make_history('source/UN/gdp_export.json', 'gdp_export')  # Доля экспорта в ВВП %
    make_history('source/UN/unemployment.json', 'unemployment')  # Уровень безработицы %
    make_history('source/UN/gdp_finance.json', 'gdp_finance')  # Доля финансовых и деловых услуг в ВВП (%)
    make_history('source/UN/consumption.json', 'consumption')  # Расходы на конечное потребление (дол)
    make_history('source/UN/Индекс потребительских цен.json', 'cpi')  # Индекс потребительских цен (к 2010)
    make_history('source/UN/cars1000.json', 'cars1000')  # Количество легковых автомобилей на 1000 человек
