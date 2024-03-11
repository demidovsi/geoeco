# https://data.tuik.gov.tr/Kategori/GetKategori?p=Construction-and-Housing-116
# https://www.tuik.gov.tr/
# https://biruni.tuik.gov.tr/bolgeselistatistik/anaSayfa.do?dil=en
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from deep_translator import GoogleTranslator
import xlrd
import json
import os
import inform
import common
import config

http_adapter = HTTPAdapter(max_retries=10)
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Cookie": "rbzid=oo6XcliuU2QC8We87874BO9GA9Pg41SRibMzr0fR4Pso9n1VAQKm0oyrnLJPVriQf9JI2FTIAoCtAV4KnJPeVGRS4rFQgN0ATx7FeG5ZsPhPER2d4NunXqSNLzg7eF+v3AlmD7G/hNyUlA5matUZg84CJQfK7RPsmDJVvhF+lDGlaBU8veOPv/ir/BtL3k6x503nnMNKWVQHCTrqwrUCWVQPIH7wmGKCucKl8DO4PwVIpJ4CmxNG0jx3/yDiSQUW; rbzsessionid=887378f03050c7c41a47db6ab65b13ff; _gid=GA1.3.1186344673.1686827652; WSS_FullScreenMode=false; deviceChannel=Default; _gat=1; _ga=GA1.1.312130872.1686827652; _ga_F5VXQ9Z7N5=GS1.1.1686827652.1.1.1686827968.0.0.0",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": "Not.A/Brand\";v=\"8\", \"Chromium\";v=\"114\", \"Google Chrome\";v=\"114\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"Windows\"",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1"
}


def make_inform():
    turkey_id = 25
    # load_provinces_html('https://turk.expert/zametki/81-il.html', 25)
    make_provinces_turkish(turkey_id)  # из файла провинций записать в базу данных
    sale_foreign(turkey_id)  # количество продаж домов иностранцам
    sale_total(turkey_id)  # количество продаж домов
    make_districts_turkish(turkey_id)  # список районов провинций


def load_provinces_html(url, country_id):
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url, headers=headers, timeout=(100, 100))
    if r.ok:
        lws = BeautifulSoup(r.text, 'html.parser').\
            find_all('table')[0].\
            find_all('tbody')[0].text.split('\n')
    else:
        print(r.text)


def do_provinces_turkish(country_id):
    # изготовление файла провинций Турции
    for data in inform.turkish_provinces:
        data['name_rus'] = GoogleTranslator(source='tr', target='ru').translate(data['name_own'])
        data['sh_name'] = GoogleTranslator(source='tr', target='en').translate(data['name_own'])
        data['country'] = country_id
    f = open('source/Турция/Провинции Турции.json', 'w', encoding='utf-8')
    with f:
        f.write(json.dumps(inform.turkish_provinces, indent=4, ensure_ascii=False))


def make_provinces_turkish(country_id):
    # из файла провинций записать в базу данных
    filename = "source/Турция/Провинции Турции.json"
    if not os.path.exists(filename):
        do_provinces_turkish(country_id)
    if not os.path.exists(filename):
        print('Нет файла ', filename)
    f = open(filename, 'r', encoding='utf-8')
    with f:
        answer = f.read()
    answer = json.loads(answer)
    list_provinces = common.load_provinces_db(country_id)
    values = list()
    for data in answer:
        for unit in list_provinces:
            if data['name_own'] == unit['name_own']:
                data['id'] = unit[id]
                break
        values.append(data)
    common.write_objects_db('provinces', values)


def sale_foreign(country_id):
    countries = common.load_countries()
    if countries is None:
        return
    filename = "source/Турция/ulke uyruklarina gore yabancilara yapilan konut satis sayilari.xls"
    df = xlrd.open_workbook_xls(filename)
    sheet = df.sheet_by_index(0)
    values = list()
    year = 2015
    for i in range(3, sheet.nrows):
        row = sheet.row_values(i)
        n = 0
        for j in range(sheet.ncols):
            if type(row[j]) == str and row[j].strip() == '':
                n += 1
        if n == sheet.ncols:  # закончена таблица, начинается подвал
            break
        if type(row[0]) == float:
            year = int(row[0])
            values.append({"date": str(year) + '-12-01', "value": row[2], "param_name": "sale_house"})
            for j in range(3, sheet.ncols):
                values.append({"date": str(year) + '-' + str(j-2) + '-01', "value": row[j],
                               'month': j-2, "param_name": "sale_house_month"})
        else:
            name_country = row[1].split('-')
            if len(name_country) < 2:
                name_country = name_country[0].strip()
                print(i)
            else:
                name_country = name_country[1].strip()
            values.append({"date": str(year) + '-12-01', "value": row[2], 'name_country': name_country})
            for j in range(3, sheet.ncols):
                values.append({"date": str(year) + '-' + str(j-2) + '-01', "value": row[j],
                               'name_country': name_country, 'month': j-2, "param_name": "sale_house_month"})
    for data in values:
        if 'name_country' in data:
            country = common.get_country_id(data['name_country'], countries)
            data['country_id'] = country
    st_query = ''
    for data in values:
        if 'country_id' not in data:  # общая по Турции
            st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});".format(
                date=data['date'], country_id=country_id, value=data['value'], schema=config.SCHEMA,
                param_name=data['param_name'])
    common.write_script_db(st_query)


def sale_total(country_id):
    countries = common.load_countries()
    if countries is None:
        return
    filename = "source/Турция/satis sekline ve satis durumuna gore konut satis sayilari.xls"
    df = xlrd.open_workbook_xls(filename)
    sheet = df.sheet_by_index(0)
    values = list()
    year = 2013
    for i in range(4, sheet.nrows):
        row = sheet.row_values(i)
        n = 0
        for j in range(sheet.ncols):
            if type(row[j]) == str and row[j].strip() == '':
                n += 1
        if n == sheet.ncols:  # закончена таблица, начинается подвал
            break
        if type(row[0]) == float and row[1].strip() == '':
            year = int(row[0])
            values.append({"date": str(year) + '-12-01', "value": row[2], "param_name": "total_sale_house"})
            values.append({"date": str(year) + '-12-01', "value": row[4], "param_name": "total_sale_house_mortage"})
            values.append({"date": str(year) + '-12-01', "value": row[7], "param_name": "total_sale_house_other"})
            values.append({"date": str(year) + '-12-01', "value": row[10], "param_name": "total_sale_house_first"})
            values.append({"date": str(year) + '-12-01', "value": row[13], "param_name": "total_sale_house_second"})
    month = 1
    for i in range(4, sheet.nrows):
        row = sheet.row_values(i)
        n = 0
        for j in range(sheet.ncols):
            if type(row[j]) == str and row[j].strip() == '':
                n += 1
        if n == sheet.ncols:  # закончена таблица, начинается подвал
            break
        if type(row[0]) == float and row[1].strip() != '' or type(row[0]) == str and row[1] != '':
            if type(row[0]) == float and row[1].strip() != '':
                year = int(row[0])
                month = 1
            values.append({"date": str(year) + '-' + str(month) + '-01', "value": row[2],
                           "param_name": "total_sale_house_month"})
            values.append({"date": str(year) + '-' + str(month) + '-01', "value": row[4],
                           "param_name": "total_sale_house_mortage_month"})
            values.append({"date": str(year) + '-' + str(month) + '-01', "value": row[7],
                           "param_name": "total_sale_house_other_month"})
            values.append({"date": str(year) + '-' + str(month) + '-01', "value": row[10],
                           "param_name": "total_sale_house_first_month"})
            values.append({"date": str(year) + '-' + str(month) + '-01', "value": row[13],
                           "param_name": "total_sale_house_second_month"})
            month += 1
    st_query = ''
    for data in values:
        st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});".format(
            date=data['date'], country_id=country_id, value=data['value'], schema=config.SCHEMA,
            param_name=data['param_name'])
    common.write_script_db(st_query)


def make_districts_turkish(country_id):
    # записать в базу данных районы провинций Турции
    token, is_ok = common.login('superadmin', common.decode('abcd', config.kirill))
    if not is_ok:
        return
    filename = "source/Турция/ilcelere gore konut satis sayilari.xls"
    if not os.path.exists(filename):
        return
    list_provinces = common.load_provinces_db(country_id)  # провинции из БД
    if list_provinces is None:
        return
    df = xlrd.open_workbook_xls(filename)
    sheet = df.sheet_by_index(0)
    for i in range(3, sheet.nrows):
        row = sheet.row_values(i)
        n = 0
        for j in range(sheet.ncols):
            if type(row[j]) == str and row[j].strip() == '':
                n += 1
        if n == sheet.ncols:  # закончена таблица, начинается подвал
            break
        name_pr = row[1].strip()
        name_pr = name_pr.replace('Adıyaman', 'Adiyaman')
        name_dis = row[2].strip()
        for data in list_provinces:
            if data['name_own'] == name_pr:  # нашли провинцию
                if 'districts' not in data:
                    data['districts'] = list()  # пустой массив
                exist = False
                for elem in data['districts']:
                    if elem['name_own'] == name_dis:
                        exist = True
                        break
                if not exist:
                    data['districts'].append({'name_own': name_dis})


def sale_foreign_tourists(country_id):
    countries = common.load_countries()
    if countries is None:
        return
    filename = "source/Турция/Кол-во туристов по странам.xls"
    df = xlrd.open_workbook_xls(filename)
    sheet = df.sheet_by_index(0)
    values = list()
    years = list()
    row = sheet.row_values(3)
    for i in range(1, len(row)):
        years.append(int(row[i]))
    # посчитаем общее количество туристов по годам
    st_query = ''
    row = sheet.row_values(4)
    for i in range(1, len(row)):
        st_query += "select {schema}.pw_his('{param_name}', '{date}', '{country_id}', {value});".format(
            date=str(years[i-1]) + '-12-31', country_id=country_id, value=row[i], schema=config.SCHEMA,
            param_name='count_tourists')
    common.write_script_db(st_query)

    # а теперь по странам
    st_query = ''
    for j in range(5, sheet.nrows):
        row = sheet.row_values(j)
        if '-' in row[0]:
            name_country = row[0].split('-')[1]
            ident = common.get_country_id(name_country, countries)
            if ident:
                for i in range(1, len(row)):
                    st_query += "select {schema}.pw_his2('{param_name}', '{date}', '{country_id}', {value}, " \
                                "{country});\n".\
                    format(date=str(years[i - 1]) + '-12-31', country_id=country_id, value=row[i], schema=config.SCHEMA,
                           param_name='count_tourists_countries', country=ident)
    common.write_script_db(st_query)
