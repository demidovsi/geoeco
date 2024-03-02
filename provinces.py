import json
import common
import config


def load_provinces_db(country_id):
    url = 'v1/select/{schema}/nsi_provinces?where=country={country_id}'.format(
        schema=config.SCHEMA, country_id=country_id)
    provinces, is_ok, status_response = common.send_rest(
        url, params={"columns": "id, name_own"})
    if not is_ok:
        print(str(provinces))
        return
    cities = json.loads(provinces)
    return cities
