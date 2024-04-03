import time

import trafaret_thread
import common
import config
import json
import datetime
import requests
import math
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
        calc_avr_day()
        return self.load_meteo()

    def send_url(self, url, api_id, lon, lat, directive="GET", qrepeat=2) -> (str, bool):
        result = False
        data = ''
        q = 0
        while not result and (q < qrepeat):
            try:
                headers = {
                    "Accept": "application/json"
                }
                mes = '?lat=' + str(lat) + '&lon=' + str(lon) + '&appid=' + api_id + '&units=metric'
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
        t0 = time.time()
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
                else:
                    common.write_log_db(
                        'ERROR', self.source, 'lat=' + str(data['lat']) + '; lon=' + str(data['lon']) + '; ' +
                        str(txt) + ': ' + par['url'] + '; Загружено {count} записей по погоде'.format(count=count_row),
                        token_admin=self.token, td=time.time() - t0)

            common.write_script_db(sql_text, token=self.token)
            self.finish_text = 'Загружено {count} записей по погоде'.format(count=count_row)
        else:
            common.write_log_db('ERROR', self.source, par['url'], token_admin=self.token)
        return count_row > 0


def get_lat_lon_cities():
    # определение и запись в таблицу городов географических координат городов, у которых они не заданы
    answer, is_ok, status = common.send_rest(
        # 'v1/select/{schema}/v_nsi_cities?where=lat is null or lon is null'.format(schema=config.SCHEMA),
        'v1/select/{schema}/v_nsi_cities'.format(schema=config.SCHEMA),
        # 'v1/select/{schema}/v_nsi_cities'.format(schema=config.SCHEMA),
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


array_keys = [
    'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_speed',
    'wind_deg', 'wind_gust', 'sunrise', 'sunset'
]


def calc_avr_day():
    """
    Рассчитать среднесуточные значения метео информации
    :return:
    """
    url = "v1/select/{schema}/get_absent_date_avr_day()?column_order=date&where=date<'{today}'".format(
        schema=config.SCHEMA, today=common.st_today())
    days, is_ok, status = common.send_rest(url)
    if not is_ok:
        common.write_log_db('ERROR', 'get_absent_date_avr_day: ' + url, str(days))
        return
    days = json.loads(days)
    for i in range(len(days)):
        date = days[i]['0']
        url = "v1/select/{schema}/get_cities_day('{dt}')".format(schema=config.SCHEMA, dt=date)
        units, is_ok, status = common.send_rest(url)
        if not is_ok:
            common.write_log_db('ERROR', 'get_cities_day: ' + url, str(days))
            return
        units = json.loads(units)
        # средние значения, которые нужно рассчитать
        sql_text = ''
        for unit in units:
            city_id = unit['0']
            value = dict()  # для среднесуточных значений
            value['duration'] = 0
            for key in array_keys:
                value[key] = 0
                value['count_' + key] = 0
                value['sco_' + key] = 0
                value['max_' + key] = None
                value['min_' + key] = None
            url = "v1/select/{schema}/his_cities_meteo?where=cities_id={city_id} and " \
                  "substring(dt::text from 1 for 10)='{date}'".format(schema=config.SCHEMA, date=date, city_id=city_id)
            values, is_ok, status = common.send_rest(url)  # значения погоды города
            if not is_ok:
                common.write_log_db('ERROR', url, str(values))
                return
            values = json.loads(values)
            count = 0
            for elem in values:
                data = elem['value']
                value['duration'] = value['duration'] + (data['sunset'] - data['sunrise']) / 3600
                count += 1
                for key in array_keys:
                    if key in data:
                        value['count_' + key] = value['count_' + key] + 1
                        value[key] = value[key] + data[key]
                        value['max_'+key] = max(data[key], value['max_'+key]) if value['max_'+key] else data[key]
                        value['min_'+key] = min(data[key], value['min_'+key]) if value['min_'+key] else data[key]
            value['duration'] = value['duration'] / count
            for key in array_keys:
                if value['count_' + key] != 0:
                    value[key] = value[key] / value['count_' + key]
            # рассчитать ско
            for elem in values:
                data = elem['value']
                for key in array_keys:
                    if key in data:
                        value['sco_' + key] = value['sco_' + key] + (data[key] - value[key])**2
            for key in array_keys:
                if value['count_' + key] != 0:
                    value['sco_' + key] = math.sqrt(value['sco_' + key]) / value['count_' + key]

            value_db = dict()
            for key in value.keys():
                if value[key] is not None:
                    value_db[key] = value[key]

            sql_text += "insert into {schema}.avr_cities_meteo_day(cities_id, date, value) values ({city_id}, " \
                        "'{dt}', '{val}'); select 1;\n".format(schema=config.SCHEMA, city_id=city_id,
                                                               val=str(value_db).replace("'", '"'), dt=date)
        common.write_script_db(sql_text)

# get_lat_lon_cities()
# calc_avr_day()