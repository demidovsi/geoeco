import common
import config
import countries as c_countries
import json
import time
from deep_translator import GoogleTranslator
import requests
from requests.adapters import HTTPAdapter

http_adapter = HTTPAdapter(max_retries=10)


def load_cities(token=None):
    url = 'v1/select/{schema}/nsi_cities'.format(schema=config.SCHEMA)
    cities, is_ok, status_response = common.send_rest(
        url, params={"columns": "id, name_rus, sh_name, population, square, country"})
    if not is_ok:
        print(str(cities))
        common.write_log_db('ERROR', 'load_cities', str(cities) + '; ' + url, token_admin=token)
        return
    cities = json.loads(cities)
    return cities
