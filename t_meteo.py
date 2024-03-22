import trafaret_thread
import common
import config
import json
import datetime
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError

http_adapter = HTTPAdapter(max_retries=10)
obj = None
api_key = common.decode('abcd', config.api_key)


class Meteo(trafaret_thread.PatternThread):
    def __init__(self, source, code_function):
        super(Meteo, self).__init__(source, code_function)

    def work(self):
        super(Meteo, self).work()
        return self.load_meteo()

    def send_url(self, url, api_id, x, y, directive="GET", qrepeat=2) -> (str, bool):
        result = False
        data = ''
        q = 0
        while not result and (q < qrepeat):
            try:
                headers = {
                    "Accept": "application/json"
                }
                mes = '?lat=' + str(x) + '&lon=' + str(y) + '&appid=' + api_id + '&units=metric'
                response = requests.request(directive, url + mes, headers=headers)
                return response.text, response.ok
                # print(url + mes)
            except HTTPError as err:
                data = f'HTTP error occurred: {err}'
                result = False
                q = q + 1
            except Exception as err:
                data = f'Other error occurred: {err}'
                result = False
                q = q + 1
        if not result:
            common.write_log_db('ERROR', self.source, url, token_admin=self.token)
        return data, result

    def load_meteo(self):
        count_row = 0
        par = trafaret_thread.get_value_config_param('compliment_txt', self.par)
        url = 'v1/select/{schema}/nsi_cities?where=need_meteo and lat is not null and lon is not null'.\
            format(schema=config.SCHEMA)
        answer, is_ok, status = common.send_rest(url)
        if is_ok:
            answer = json.loads(answer)
            dt = datetime.datetime.utcnow()
            st = common.time_for_sql(dt, False)
            sql_text = ''
            # https: // api.openweathermap.org / data / 2.5 / onecall
            for data in answer:
                txt, result = self.send_url(par['url'], par['api'], data['lon'], data['lat'])
                if result:
                    count_row += 1
                    val = json.loads(txt)['current']
                    sql_text += "insert into {schema}.his_cities_meteo (cities_id, dt, value) values ({city_id}, " \
                                "'{dt}', '{val}'); select 1;".format(schema=config.SCHEMA, city_id=data['id'],
                                                                     val=str(val).replace("'", '"'), dt=st)
            common.write_script_db(sql_text, token=self.token)
            self.finish_text = 'Загружено {count} записей по погоде'.format(count=count_row)
            # txt, result, status = common.send_rest('v1/execute', 'PUT', params=txt, token_user=self.token)
            # if not result or ('error_sql' in txt):
            #     common.write_log_db('ERROR', self.source, par['url'], token_admin=self.token)
        else:
            common.write_log_db('ERROR', self.source, par['url'], token_admin=self.token)
        return count_row > 0


def get_lat_lon_cities():
    # определение и запись в таблицу городов географических координат городов, у которых они не заданы
    answer, is_ok, status = common.send_rest(
        'v1/select/{schema}/v_nsi_cities?type_view=view&where=lat is null or lon is null'.format(schema=config.SCHEMA),
        params={'columns': 'id, sh_name, name_country'})
    if not is_ok:
        print(answer)
    else:
        answer = json.loads(answer)
        sql_text = ''
        count_row = 0
        count = 0
        for city in answer:
            url = "https://maps.googleapis.com/maps/api/geocode/json?address={city}, {country}&key={api_key}".\
                format(api_key=api_key, city=city['sh_name'], country=city['name_country'])
            response = requests.get(url)
            data = response.json()
            if data['status'] == 'OK':
                count += 1
                count_row += 1
                location = data['results'][0]['geometry']['location']
                sql_text += 'update {schema}.nsi_cities set lat={lat}, lon={lon} where id={id}; select 1;\n'.format(
                    schema=config.SCHEMA, lat=location['lat'], lon=location['lng'], id=city['id'])
                if count >= 100:
                    common.write_script_db(sql_text)
                    sql_text =''
                    count = 0
                    print('Записано', count_row, 'из', len(answer))
        common.write_script_db(sql_text)


get_lat_lon_cities()