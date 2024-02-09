import requests
from requests.adapters import HTTPAdapter
import countries as c_countries
import json

http_adapter = HTTPAdapter(max_retries=10)


def load_form_government_html(url, param_name):
    session = requests.Session()
    session.mount(url, http_adapter)
    r = session.get(url)
    if r.ok:
        countries = c_countries.load_countries()
        lws = r.text.split('<table class="tablepress tablepress-id-520 merkator-4-05">')[0].split('tbody>')[1].\
            split('</table>')[0].split('<tr>')[2:-1]
