import trafaret_thread
import common
import config
import json
import time
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from deep_translator import GoogleTranslator

http_adapter = HTTPAdapter(max_retries=10)
obj = None
headers0 = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/108.0.0.0",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Cookie": "__cf_bm=w194VGYRG_g5SzLiQK1d9Sk0TQlYNvNr3gIOuW.zKyY-1711971697-1.0.1.1-BoytKWhr6cKlvDQrel3KZ."
              "NzUKmRFeOUd5hcShGFA0HOea5_HkzxJMPM2zGR0b8jKWW7RPDJFUCVhrgMlwznCQ; _ga=GA1.1.616200987.1711971699; "
              "_ga_WXDNDRLF1Q=GS1.1.1711971699.1.0.1711971699.0.0.0; cf_clearance=Zjb.p5yyBrlHAJ0AbrR7."
              "RKz6FTCFu45Pc6Q1e0a3U0-1711971698-1.0.1.1-D3w3Zd.iRh1JPCDvEUbeZuzy5aL9ax9nlfCeo34FdFrlEckVqklceOcs"
              "VOsHS37kbyEnnkjril2GnWB2ltcFmA; __gads=ID=ea60d8e9dbdcd5b2:T=1711971698:RT=1711971698:S=ALNI_"
              "Mbu9Ar0EHWtduGKtSinifoTT2U6Eg; __gpi=UID=00000d53dbad2427:T=1711971698:"
              "RT=1711971698:S=ALNI_MYjnHYdyiDuZpobyW4SiC1BX8xwTw; __eoi=ID=636acef33e5033d0:"
              "T=1711971698:RT=1711971698:S=AA-AfjZNGxBMFtxgxJPvK2vJQ4ZH",
    "Cache-Control": "max-age=0",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Opera";v="108"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"Windows\"",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1"
}


class CityFacts(trafaret_thread.PatternThread):

    def __init__(self, source, code_function):
        super(CityFacts, self).__init__(source, code_function)

    def work(self):
        super(CityFacts, self).work()
        return self.load_list_indicators()

    def load_html(self, url):
        t = time.time()
        session = requests.Session()
        session.mount(url, http_adapter)
        r = session.get(url, headers=headers, timeout=(100, 100))
        try:
            if r.ok:
                return r.text
            else:
                common.write_log_db(
                    'Error', self.source, str(r.status_code) + '; ' + str(r.text), td=time.time() - t,
                    file_name=common.get_computer_name(), token_admin=self.token)
        except Exception as err:
            st = f"{err}\n" + url
            common.write_log_db(
                'Exception', self.source, st, td=time.time() - t,
                file_name=common.get_computer_name(), token_admin=self.token)

    def load_list_indicators(self):
        url = "v1/select/{schema}/nsi_import?where=sh_name='{code_function}' and active".format(
            schema=config.SCHEMA, code_function=self.code_parser)
        answer, is_ok, status = common.send_rest(url)
        result = 0
        if is_ok:
            answer = json.loads(answer)
            data = answer[0]
            countries = common.load_from_db('countries', where="upper(sh_name)='{sh_name}'".format(
                sh_name=data['param_name'].upper()))
            if countries is None or len(countries) == 0:
                print('Отсутствует страна ' + data['param_name'])
                return False
            country_id = countries[0]['id']
            lws = self.load_html(data['code'])
            if lws is None:
                return False
            for data in answer:
                if data['object_code'] == 'countries':  # записать информацию по населению
                    if self.get_countries(data, country_id, lws):
                        result += 1
        return result != 0

    def get_countries(self, data, country_id, lws):
        lws = BeautifulSoup(lws, 'html.parser').find_all('table')[0].find_all('tr')
        for trs in lws:
            tds = trs.find_all('td')
            st = tds[0].text

# CityFacts('Население city_facts', 'city_facts').start()
# while True:
#     time.sleep(5)

# import requests

url = "https://ru.city-facts.com/ecuador/population"

payload = {}
headers = {
  'authority': 'ru.city-facts.com',
  'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
  'accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
  'cache-control': 'max-age=0',
  'cookie': '__cf_bm=w194VGYRG_g5SzLiQK1d9Sk0TQlYNvNr3gIOuW.zKyY-1711971697-1.0.1.1-BoytKWhr6cKlvDQrel3KZ.NzUKmRFeOUd5hcShGFA0HOea5_HkzxJMPM2zGR0b8jKWW7RPDJFUCVhrgMlwznCQ; _ga=GA1.1.616200987.1711971699; _ga_WXDNDRLF1Q=GS1.1.1711971699.1.0.1711971699.0.0.0; cf_clearance=Zjb.p5yyBrlHAJ0AbrR7.RKz6FTCFu45Pc6Q1e0a3U0-1711971698-1.0.1.1-D3w3Zd.iRh1JPCDvEUbeZuzy5aL9ax9nlfCeo34FdFrlEckVqklceOcsVOsHS37kbyEnnkjril2GnWB2ltcFmA; __gads=ID=ea60d8e9dbdcd5b2:T=1711971698:RT=1711971698:S=ALNI_Mbu9Ar0EHWtduGKtSinifoTT2U6Eg; __gpi=UID=00000d53dbad2427:T=1711971698:RT=1711971698:S=ALNI_MYjnHYdyiDuZpobyW4SiC1BX8xwTw; __eoi=ID=636acef33e5033d0:T=1711971698:RT=1711971698:S=AA-AfjZNGxBMFtxgxJPvK2vJQ4ZH',
  'if-modified-since': 'Mon, 01 Apr 2024 11:37:08 GMT',
  'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Opera";v="108"',
  'sec-ch-ua-mobile': '?0',
  'sec-ch-ua-platform': '"Windows"',
  'sec-fetch-dest': 'document',
  'sec-fetch-mode': 'navigate',
  'sec-fetch-site': 'none',
  'sec-fetch-user': '?1',
  'upgrade-insecure-requests': '1',
  'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/108.0.0.0',
  'Referer': '',
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 OPR/108.0.0.0',
  'Origin': 'https://ru.city-facts.com',
  'content-type': 'application/x-www-form-urlencoded',
  'origin': 'https://ru.city-facts.com',
  'referer': 'https://ru.city-facts.com/ecuador/population',
  'Accept': 'application/json, text/javascript, */*; q=0.01',
  'content-length': '0',
  'x-client-data': 'CI6EywE=',
  'Upgrade-Insecure-Requests': '1',
  'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
  'Connection': 'keep-alive',
  'Sec-Fetch-Dest': 'image',
  'Sec-Fetch-Mode': 'no-cors',
  'Sec-Fetch-Site': 'cross-site',
  'attribution-reporting-eligible': 'event-source',
  'access-control-request-headers': 'attribution-reporting-eligible',
  'access-control-request-method': 'GET'
}

# response = requests.request("GET", url, headers=headers, data=payload)

# print(response.text)

