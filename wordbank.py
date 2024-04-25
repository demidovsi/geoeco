# https://data.worldbank.org/indicator
import time

import config
import common
import countries as c_countries
import json
import wbgapi as wb
from deep_translator import GoogleTranslator


def load_data(indicator, param_name, clear_table=False):
    coefficient = 1
    url = "v1/select/{schema}/nsi_import?where=sh_name='wb' and code='{indicator}'".format(
        schema=config.SCHEMA, indicator=indicator)
    answer, is_ok, status = common.send_rest(url)
    if not is_ok:
        print(str(answer))
        return
    data = json.loads(answer)[0]
    if data['coefficient']:
        coefficient = data['coefficient']
    param_name = data['param_name']
    # узнать сколько элементов имеется всего
    url = "{url_wordbank}{indicator}?format=json&page=1&per_page=1000".format(
        url_wordbank=config.url_wordbank, indicator=indicator)
    answer, is_ok, status = common.send_rest_url(url)
    if is_ok:
        answer = json.loads(answer)
        total = answer[0]['total']
        url = "{url_wordbank}{indicator}?format=json&page={page}&per_page={total}".format(
            url_wordbank=config.url_wordbank, indicator=indicator, page=1, total=total)
        answer, is_ok, status = common.send_rest_url(url)  # прочитать все
        if is_ok:
            countries = common.load_countries()
            answer = json.loads(answer)
            txt, is_ok, token, lang = common.login_superadmin()
            if clear_table:
                # таблицу очистить
                common.send_rest(
                    'v1/NSI/script/execute', 'PUT', "truncate table {schema}.his_countries_{param_name}".format(
                        schema=config.SCHEMA, param_name=param_name), lang='en', token_user=token)
            st_query = ''
            count = 0
            for unit in answer[1]:
                if unit['value']:
                    code = unit['countryiso3code']
                    country_id = None
                    for elem in countries:
                        if elem['code'] == code:
                            country_id = elem['id']
                            break
                    if country_id:  # есть такая страна
                        value = unit['value']
                        year = int(unit['date'])
                        st_query += "select {schema}.pw_his('{param_name}', '{date}-12-01', {country_id}, {value}); select 1;\n".\
                            format(param_name=param_name, date=year, country_id=country_id, value=value * coefficient,
                                   schema=config.SCHEMA)
                        count += 1
                        if count > 1000:
                            common.write_script_db(st_query)
                            st_query = ''
                            count = 0
            common.write_script_db(st_query)

    else:
        print(str(answer))


def load_inform():
    load_data('AG.AGR.TRAC.NO', 'agr_machinery')  # Машин, тракторов на 100 кв. км
    load_data('AG.CON.FERT.ZS', 'arg_fertilizer')  # Расход удобрений (кг/га)
    load_data('AG.LND.AGRI.K2', 'agr_square')  # площадь сельских территорий
    load_data('AG.LND.AGRI.ZS', 'agr_square_percent')  # Процент сельских территорий от общей территории
    load_data('AG.LND.FRST.K2', 'forest_square')  # Площадь лесов (кв. км)
    load_data('AG.LND.FRST.ZS', 'forest_square_percent')  # Процент лесов от общей территории
    load_data('AG.LND.ARBL.ZS', 'agr_arable_land_percent')  # Процент пахотных земель
    load_data('AG.LND.ARBL.HA', 'agr_arable_land')  # Пашня (га)
    load_data('AG.LND.PRCP.MM', 'agr_avr_prec_depth')  # Среднее кол-во осадков по глубине (мм в год)
    load_data('AG.PRD.CREL.MT', 'agr_cereal_production')  # Производство зерновых (метрические тонны)
    load_data('AG.YLD.CREL.KG', 'agr_cereal_yield')  # Урожайность зерновых (кг/га)
    load_data('DT.DOD.DPPG.CD', 'ppg')  # стоимость внешнего долга (дол)
    load_data('DT.DOD.MWBG.CD', 'ibrd', True)  # Кредиты МБРР и МАР дол
    load_data('DT.DOD.DPNG.CD', 'png')  # Объем внешнего долга, частный негарантированный (дол)
    load_data('EN.POP.DNST', 'pop_dnst')  # Плотность населения (чел на кв.км территории)
    load_data('EN.URB.LCTY.UR.ZS', 'urb_lcty')  # Численность крупнейшего города (% от городского населения)
    load_data('EN.URB.MCTY.TL.ZS', 'urb_mln_procent')  # Численность населения городов (с более 1 млн жителей) % от общей численности
    load_data('FI.RES.TOTL.CD', 'rezerv', True)  # Общие резервы (с золотом) дол
    load_data('FP.CPI.TOTL.ZG', 'inflation')  # Годовая инфляция %
    load_data('FR.INR.DPST', 'deposit_rate')  # Процентная ставка по депозиту %
    load_data('GC.DOD.TOTL.GD.ZS', 'cgd_percent')  # долг государства в % к ВВП
    load_data('GC.TAX.TOTL.GD.ZS', 'taxrevenue_per')  # Налоговые поступления (% ВВП)
    load_data('IC.TAX.TOTL.CP.ZS', 'taxtotal_per')  # Общая ставка налога и взноса (% от прибыли)
    load_data('IS.RRS.TOTL.KM', 'total_railway')  # Длина железных дорог (км)
    load_data('IS.AIR.DPRT', 'total_air')  # Количество авиа-рейсов
    load_data('IT.NET.SECR', 'it_net_secr')  # Количество безопасных интернет-серверов
    load_data('IT.NET.SECR.P6', 'it_net_mln')  # Количество безопасных интернет-серверов на 1 миллион человек
    load_data('IT.NET.USER.ZS', 'inet_user_per')  # Количество пользующихся интернетом (% от населения)
    load_data('IT.CEL.SETS.P2', 'cel_sets')  # Количество симок (на 100 человек)
    load_data('MS.MIL.XPND.GD.ZS', 'military_gdp_per')  # Доля военных расходов в ВВП %
    load_data('NE.CON.TOTL.CD', 'consumption', True)  # Расходы на конечное потребление (дол)
    load_data('NE.IMP.GNFS.ZS', 'gdp_import')  # Импорт товаров и услуг (% к ВВП)
    load_data('NE.EXP.GNFS.ZS', 'gdp_export')  # Импорт товаров и услуг (% к ВВП)
    load_data('NV.AGR.TOTL.ZS', 'gdp_agriculture')  # Доля сельского хоз-ва в ВВП %
    load_data('NY.GDP.MKTP.PP.CD', 'gdpp')  # ВВП, ППС (дол)
    load_data('NY.GDP.PCAP.PP.CD', 'gdp_pop')  # ВВП на душу населения (дол)
    load_data('NY.GDP.MKTP.KD.ZG', 'gdp_growth')  # Годовой рост ВВП %
    load_data('NY.GDP.PCAP.KD.ZG', 'gdp_growth_per_percent')  # Рост ВВП на душу населения (годовой %)
    load_data('NY.GDP.TOTL.RT.ZS', 'gdp_tnrr_percent')  # Общая рента от природных ресурсов (% ВВП)
    load_data('SL.UEM.TOTL.NE.ZS', 'unemployment')  # уровень безработицы
    load_data('SP.POP.TOTL', 'pops')  # население стран
    load_data('SP.RUR.TOTL', 'agr_population')  # количество сельских жителей
    load_data('SL.AGR.EMPL.ZS', 'agr_employment_percent')  # Занятость в сх (% от общей занятости)
    load_data('SP.URB.TOTL', 'urb_total')  # Городское население
    load_data('SP.URB.TOTL.IN.ZS', 'urb_total_percent')  # Городское население (% от населения)
    load_data('SP.RUR.TOTL.ZS', 'agr_pop_percent')  # Сельское населения % от общего населения
    load_data('SP.DYN.TFRT.IN', 'fert_coeff')  # Коэффициент рождаемости, всего (рождений на одну женщину)
    load_data('SP.DYN.CDRT.IN', 'death_rate')  # Смертность, общая (на 1000 человек)
    load_data('SP.DYN.IMRT.IN', 'mort_rate_inf')  # Смертность младенцев, общая (на 1000 живорождений)
    load_data('SP.ADO.TFRT', 'fert_inf')  # Подростковая рождаемость
    #  load_data('MS.MIL.XPND.CD', 'military_total')  # Военные расходы (дол)


def import_indicators():
    def get_id(code):
        for unit in indicators:
            if unit['code'] == code:
                return unit['id']

    array = wb.series.info().table()
    values = list()
    indicators = common.load_from_db('import', "sh_name='wb'")
    for data in array:
        if data[0]:
            param = {"sh_name": "wb", "code": data[0], "name": data[1], "object_code": "countries",
                     "name_rus": GoogleTranslator(source='en', target='ru').translate(data[1])}
            indicator_id = get_id(param['code'])
            if indicator_id:
                param['id'] = indicator_id
            values.append(param)
    common.write_objects_db('import', values)


# load_data('SP.DYN.AMRT.FE', 'sp_dyn_amrt_fe')  # Смертность взрослых женщин (на 1000 взрослых женщин)
# load_data('SP.DYN.AMRT.MA', 'sp_dyn_amrt_ma')  # Смертность взрослых мужчин (на 1000 взрослых мужчин)
# load_data('SP.DYN.CBRT.IN', 'sp_dyn_cbrt_in')  # Коэффициент рождаемости, общий (на 1000 человек)
# load_data('SP.DYN.LE00.FE.IN', 'sp_dyn_le00_fe_in')  # Ожидаемая продолжительность жизни при рождении, женщины (лет)
# load_data('SP.DYN.LE00.MA.IN', 'sp_dyn_le00_ma_in')  # Ожидаемая продолжительность жизни при рождении, мужчины (лет)
# load_data('SP.DYN.WFRT', 'sp_dyn_wfrt')  # Желаемый коэффициент рождаемости (рождений на одну женщину)
# load_data('SP.DYN.TO65.FE.ZS', 'sp_dyn_to65_fe_zs')  # Доживаемость до 65 лет, женщины (% когорты)
# load_data('SP.DYN.TO65.MA.ZS', 'sp_dyn_to65_ma_zs')  # Доживаемость до 65 лет, мужчины (% когорты)
# load_data('SP.POP.0014.TO.ZS', 'sp_pop_0014_to_zs')  # Население в возрасте 0–14 лет (% от общей численности населения)
# load_data('SP.POP.1564.TO.ZS', 'sp_pop_1564_to_zs')  # Население в возрасте 15–64 лет (% от общей численности населения)
# load_data('SP.POP.65UP.TO.ZS', 'sp_pop_65up_to_zs')  # Население в возрасте 65 лет и старше (% от общей численности населения)
# load_data('BN.KLT.DINV.CD', '')  # Прямые иностранные инвестиции, чистые (платежный баланс, текущие доллары США)
# load_data('BX.KLT.DINV.CD.WD', '')  # Прямые иностранные инвестиции, чистый приток (платежный баланс, текущие доллары США)
# load_data('BM.KLT.DINV.CD.WD', '')  # Прямые иностранные инвестиции, чистый отток (платежный баланс, текущие доллары США)
load_data('FP.CPI.TOTL', '')  # Индекс потребительских цен (2010 г. = 100)
