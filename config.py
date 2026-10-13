# Настройки и секреты читаются из переменных окружения / файла .env (шаблон — .env.example).
# В Docker-контейнере на сервере переменные передаются через --env-file /opt/geoeco/.env
import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))


def _require(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f'Не задана переменная окружения {name} (см. .env.example)')
    return value


URL = os.getenv('API_URL', 'http://159.223.0.234:5000/')
SCHEMA = os.getenv('SCHEMA', 'eco')
kirill = _require('ENCODED_PASSWORD')  # пароль superadmin, закодированный common.encode('abcd', ...)

url_wordbank = os.getenv('URL_WORLDBANK', 'https://api.worldbank.org/v2/country/all/indicator/')
url_numbeo_cost = os.getenv(
    'URL_NUMBEO', 'https://www.numbeo.com/cost-of-living/prices_by_country.jsp?displayCurrency=USD&itemId=')
# url_trading = "https://ru.tradingeconomics.com/country-list/"

api_key = os.getenv('GOOGLE_API_KEY_ENCODED', '')  # ключ Google Maps, закодированный common.encode('abcd', ...)
apilayer_key = os.getenv('APILAYER_API_KEY', '')  # курсы валют (t_courses, countries)
us_census_key = os.getenv('US_CENSUS_API_KEY', '')

#https://w3.unece.org/PXWeb/ru/TableDomains/
#https://data.worldbank.org/indicator
