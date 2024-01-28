# Сервисы по закачке данных в базе ECO (гео-экономика)
# https://www.numbeo.com/cost-of-living/prices_by_country.jsp
# https://restcountries.com/v3.1/all
#https://api.worldbank.org/v2/country/all/indicator/NY.ADJ.NNTY.KD.ZG?format=json&page=1&date=2022&per_page=10
#https://api.worldbank.org/v2/country/all/indicator/GC.DOD.TOTL.GD.ZS?format=json&page=1&per_page=2
import common
import countries
import wordbank
import json

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # countries.receive_inform()
    # countries.load_tld()
    # countries.load_languages()
    # countries.load_currencies()
    # countries.load_courses(common.st_today())
    # countries.load_list_countries()
    # countries.make_countries()

    # f = open('consumption.json', 'r', encoding='utf-8')
    # with f:
    #     answer = f.read()
    # answer = json.loads(answer)
    # f = open('consumption.json', 'w', encoding='utf-8')
    # with f:
    #     f.write(json.dumps(answer, indent=4, ensure_ascii=False, sort_keys=True))

    # countries.make_history_UN('GDPP.json', 'gdpp')
    # countries.make_history_UN('gdp_import.json', 'gdp_import')
    # countries.make_history_UN('gdp_export.json', 'gdp_export')
    # countries.make_history_UN('unemployment.json', 'unemployment')
    # countries.make_history_UN('gdp_finance.json', 'gdp_finance')
    # countries.make_history_UN('gdp_agriculture.json', 'gdp_agriculture')
    # countries.make_history_UN('source/consumption.json', 'consumption')
    # countries.make_history_text('population_his.txt', 'pw_his_population')
    # countries.make_history_text('Consumer price index.txt', 'cpi')
    # countries.make_history_text('GDP на душу.txt', 'gdp_pop')

    # countries.make_history_numbeo('water.json', 'water')
    # countries.make_history_numbeo('eggs.json', 'eggs')

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
    wordbank.load_data('SL.AGR.EMPL.ZS', 'agr_employment_percent')  # Занятость в сх (% от общей занятости)
