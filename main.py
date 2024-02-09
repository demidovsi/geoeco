# Сервисы по закачке данных в базе ECO (гео-экономика)
# https://www.numbeo.com/cost-of-living/prices_by_country.jsp
# https://restcountries.com/v3.1/all
#https://api.worldbank.org/v2/country/all/indicator/NY.ADJ.NNTY.KD.ZG?format=json&page=1&date=2022&per_page=10
import common
import countries
import wordbank
import cities
import government
import bigmac
import json
import time
import tables_html

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # countries.receive_inform()  # взять информацию из API и записать в файл result.json
    # countries.load_tld()  # суффиксы доменов
    # countries.load_languages()  # языки мира
    # countries.load_currencies() # валюты мира
    # countries.load_courses(common.st_today())  # загрузить курсы валют на текущий день
    # countries.load_list_countries()  # загрузить страны для границ
    # countries.make_countries()  # прочитать не историческую информацию по странам

    # filename = 'source/cars1000.json'
    # f = open(filename, 'r')
    # with f:
    #     answer = f.read()
    # answer = json.loads(answer)
    # f = open(filename, 'w', encoding='utf-8')
    # with f:
    #     f.write(json.dumps(answer, indent=4, ensure_ascii=False, sort_keys=True))

    # countries.make_history_un('source/Доля торговли, гост.услуг, транспорта и связи в ВВП (gdp_market).json', 'gdp_market')  # Доля торговли, гост.услуг, транспорта и связи в ВВП (%)
    # countries.make_history_un('source/Доля строительства в ВВП (gdp_building).json', 'gdp_building')  # Доля строительства в ВВП (%)
    # countries.make_history_un('source/GDPP.json', 'gdpp')  # ВВП(ППС) (дол)
    # countries.make_history_un('source/gdp_import.json', 'gdp_import')  # Доля импорта в ВВП %
    # countries.make_history_un('source/gdp_export.json', 'gdp_export')  # Доля экспорта в ВВП %
    # countries.make_history_un('source/unemployment.json', 'unemployment')  # Уровень безработицы %
    # countries.make_history_un('source/gdp_finance.json', 'gdp_finance')  # Доля финансовых и деловых услуг в ВВП (%)
    # countries.make_history_un('source/gdp_agriculture.json', 'gdp_agriculture')  # Доля сельского хоз-ва в ВВП %
    # countries.make_history_un('source/consumption.json', 'consumption')  # Расходы на конечное потребление (дол)
    # countries.make_history_un('source/Индекс потребительских цен.json', 'cpi')  # Индекс потребительских цен (к 2010)
    # countries.make_history_un('source/cars1000.json', 'cars1000')  # Количество легковых автомобилей на 1000 человек

    # wordbank.load_data('DT.DOD.DPPG.CD', 'ppg')  # стоимость внешнего долга (дол)
    # wordbank.load_data('DT.DOD.MWBG.CD', 'ibrd', True)  # Кредиты МБРР и МАР дол
    # wordbank.load_data('FI.RES.TOTL.CD', 'rezerv', True)  # Общие резервы (с золотом) дол
    # wordbank.load_data('FP.CPI.TOTL.ZG', 'inflation')  # Годовая инфляция %
    # wordbank.load_data('GC.DOD.TOTL.GD.ZS', 'cgd_percent')  # долг государства в % к ВВП
    # wordbank.load_data('NE.CON.TOTL.CD', 'consumption', True)  # Расходы на конечное потребление (дол)
    # wordbank.load_data('NE.IMP.GNFS.ZS', 'gdp_import')  # Импорт товаров и услуг (% к ВВП)
    # wordbank.load_data('NE.EXP.GNFS.ZS', 'gdp_export')  # Импорт товаров и услуг (% к ВВП)
    # wordbank.load_data('NY.GDP.MKTP.PP.CD', 'gdpp')  # ВВП, ППС (дол)
    # wordbank.load_data('NY.GDP.PCAP.PP.CD', 'gdp_pop')  # ВВП на душу населения (дол)
    # wordbank.load_data('NY.GDP.MKTP.KD.ZG', 'gdp_growth')  # Годовой рост ВВП %
    # wordbank.load_data('SL.UEM.TOTL.NE.ZS', 'unemployment')  # уровень безработицы
    # wordbank.load_data('SP.POP.TOTL', 'pops')  # население стран
    # wordbank.load_data('SP.RUR.TOTL', 'agr_population')  # количество сельских жителей
    # wordbank.load_data('AG.LND.AGRI.K2', 'agr_square')  # площадь сельских территорий
    # wordbank.load_data('AG.LND.AGRI.ZS', 'agr_square_percent')  # Процент сельских территорий от общей территории
    # wordbank.load_data('AG.LND.FRST.K2', 'forest_square')  # Площадь лесов (кв. км)
    # wordbank.load_data('AG.LND.FRST.ZS', 'forest_square_percent')  # Процент лесов от общей территории
    # wordbank.load_data('AG.LND.ARBL.ZS', 'agr_arable_land_percent')  # Процент пахотных земель
    # wordbank.load_data('AG.LND.ARBL.HA', 'agr_arable_land')  # Пашня (га)
    # wordbank.load_data('AG.LND.PRCP.MM', 'agr_avr_prec_depth')  # Среднее кол-во осадков по глубине (мм в год)
    # wordbank.load_data('AG.PRD.CREL.MT', 'agr_cereal_production')  # Производство зерновых (метрические тонны)
    # wordbank.load_data('AG.YLD.CREL.KG', 'agr_cereal_yield')  # Урожайность зерновых (кг/га)
    # wordbank.load_data('AG.CON.FERT.ZS', 'arg_fertilizer')  # Расход удобрений (кг/га)
    # wordbank.load_data('AG.CON.FERT.ZS', 'agr_machinery')  # Машин, тракторов на 100 кв. км
    # wordbank.load_data('SL.AGR.EMPL.ZS', 'agr_employment_percent')  # Занятость в сх (% от общей занятости)
    # wordbank.load_data('IS.RRS.TOTL.KM', 'total_railway')  # Длина железных дорог (км)
    # wordbank.load_data('IS.AIR.DPRT', 'total_air')  # Количество авиа-рейсов
    # wordbank.load_data('IT.NET.SECR', 'it_net_secr')  # Количество безопасных интернет-серверов
    # wordbank.load_data('IT.NET.SECR.P6', 'it_net_mln')  # Количество безопасных интернет-серверов на 1 миллион человек
    # wordbank.load_data('IT.NET.USER.ZS', 'inet_user_per')  # Количество пользующихся интернетом (% от населения)
    # wordbank.load_data('IT.CEL.SETS.P2', 'cel_sets')  # Количество симок (на 100 человек)
    # wordbank.load_data('EN.POP.DNST', 'pop_dnst')  # Плотность населения (чел на кв.км территории)
    # wordbank.load_data('EN.URB.LCTY.UR.ZS', 'urb_lcty')  # Численность крупнейшего города (% от городского населения)
    # wordbank.load_data('SP.URB.TOTL', 'urb_total')  # Городское население
    # wordbank.load_data('SP.URB.TOTL.IN.ZS', 'urb_total_percent')  # Городское население (% от населения)
    # wordbank.load_data('EN.URB.MCTY.TL.ZS', 'urb_mln_procent')  # Численность населения городов (с более 1 млн жителей) % от общей численности
    # wordbank.load_data('SP.RUR.TOTL.ZS', 'agr_pop_percent')  # Сельское населения % от общего населения
    # wordbank.load_data('NY.GDP.PCAP.KD.ZG', 'gdp_growth_per_percent')  # Рост ВВП на душу населения (годовой %)
    # wordbank.load_data('NY.GDP.TOTL.RT.ZS', 'gdp_tnrr_percent')  # Общая рента от природных ресурсов (% ВВП)
    # wordbank.load_data('DT.DOD.DPNG.CD', 'png')  # Объем внешнего долга, частный негарантированный (дол)
    # wordbank.load_data('IC.TAX.TOTL.CP.ZS', 'taxtotal_per')  # Общая ставка налога и взноса (% от прибыли)
    # wordbank.load_data('GC.TAX.TOTL.GD.ZS', 'taxrevenue_per')  # Налоговые поступления (% ВВП)
    # wordbank.load_data('SP.DYN.TFRT.IN', 'fert_coeff')  # Коэффициент рождаемости, всего (рождений на одну женщину)
    # wordbank.load_data('SP.DYN.CDRT.IN', 'death_rate')  # Смертность, общая (на 1000 человек)
    # wordbank.load_data('SP.DYN.IMRT.IN', 'mort_rate_inf')  # Смертность младенцев, общая (на 1000 живорождений)
    # wordbank.load_data('SP.ADO.TFRT', 'fert_inf')  # Подростковая рождаемость
    # wordbank.load_data('FR.INR.DPST', 'deposit_rate')  # Процентная ставка по депозиту %

    # for year in range(2012, time.gmtime().tm_year + 1):
    #     lws = tables_html.load_html(
    #         "https://www.numbeo.com/crime/rankings_by_country.jsp?title={year}".format(year=year))
    #     if lws:
    #         tables_html.make_numbeo_crime(lws, year, 'crime', 3, 0, 1)  # индекс преступности

    # for year in range(2012, time.gmtime().tm_year + 1):
    #     lws = tables_html.load_html(
    #         "https://www.numbeo.com/quality-of-life/rankings_by_country.jsp?title={year}".format(year=year))
    #     if lws:
            # tables_html.make_numbeo_crime(lws, year, 'life_index', 10, 0, 1)  # Индекс качества жизни
            # tables_html.make_numbeo_crime(lws, year, 'ability_index', 10, 0, 2)  # Индекс покупательной способности
            # tables_html.make_numbeo_crime(lws, year, 'safety_index', 10, 0, 3)  # Индекс безопасности (криминал)
            # tables_html.make_numbeo_crime(lws, year, 'health_index', 10, 0, 4)  # Индекс здравоохранения
            # tables_html.make_numbeo_crime(lws, year, 'cost_index', 10, 0, 5)  # Индекс стоимости жизни (% к Нью-Йорку)
            # tables_html.make_numbeo_crime(lws, year, 'pp_to_income_ratio', 10, 0, 6)  # Соотношение цены дома к доходу
            # tables_html.make_numbeo_crime(lws, year, 'trafic_time_index', 10, 0, 7)  # Индекс времени в пути на дорогах
            # tables_html.make_numbeo_crime(lws, year, 'pollution_index', 10, 0, 8)  # Индекс загрязнения
            # tables_html.make_numbeo_crime(lws, year, 'climate_index', 10, 0, 9)  # Индекс климата

    # tables_html.make_numbeo_salary(105, 'av_salary_net')  # среднемесячная зарплата (дол) после налогов
    # tables_html.make_numbeo_salary(106, 'mortage_per')  # Процентная ставка по ипотеке % на 20 лет (города)
    # tables_html.make_numbeo_salary(101, 'price_sm_aoc')  # Цена квадратного метра жилья не в центре города
    tables_html.make_numbeo_salary(100, 'price_sm_acc')  # Цена квадратного метра жилья в центре города

    # cities.load_cities_html("https://www.statdata.ru/largestcities_world")  # список крупнейших городов

    # ------------------ не задокументировано
    # government.load_form_government_html(
    #     'https://merkator.org.ua/ru/spravochnik/formy-pravleniya-stran-mira', 'government')
    # bigmac.load_bm()  # !!! отдельная песня сделанная Кириллом через эксель и экспорт в свою базу

    # cities.make_form_government('source/Формы правления государств.txt')  # формы государства
    # cities.make_type_government('source/Формы государственного устройства.txt')  # типы государства

    # countries.make_history_numbeo('source/water.json', 'water')  # Цена 1.5 литра воды (регуляр)
    # countries.make_history_numbeo('source/eggs.json', 'eggs')  # Цена 12 яиц (регуляр)
    # countries.load_courses('2024-02-07')  # загрузка курсов валюты стран за указанные сутки
