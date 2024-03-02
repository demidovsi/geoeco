from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter
from deep_translator import GoogleTranslator
import common
import config
import countries as c_countries

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


def load_html(indicator):
    url = config.url_trading + indicator
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url, headers=headers, timeout=(100, 100))
    if r.ok:
        lws = BeautifulSoup(r.text, 'html.parser').find_all('table')[0].find_all('tr')[1:]
        return lws
    else:
        print(r.text)
        return []


def load_data_html(indicator):
    param_name = common.get_param_name_by_indicator(indicator, 'trading')
    if param_name is None:
        print('Не удалось определить имя параметра для', indicator)
        return
    countries = c_countries.load_countries()
    if countries is None:
        print('Отсутствуют страны')
        return
    answer = load_html(indicator)
    st_query = ''
    for data in answer:
        values = data.text.split('\n')
        name_country = values[3].strip()
        value_prev = values[5]
        value = values[6]
        date = values[7]
        date_prev = date.split('-')
        date_prev = str(int(date_prev[0])-1) + '-' + date_prev[1]
        country_id = common.get_country_id(name_country, countries)
        if country_id:
            st_query += "select {schema}.pw_his('{param_name}', '{date}', {country_id}, {value});\n". \
                format(param_name=param_name, date=date + '-01', country_id=country_id, value=value,
                       schema=config.SCHEMA)
            st_query += "select {schema}.pw_his('{param_name}', '{date}', {country_id}, {value});\n". \
                format(param_name=param_name, date=date_prev + '-01', country_id=country_id, value=value_prev,
                       schema=config.SCHEMA)
    common.write_script_db(st_query)
