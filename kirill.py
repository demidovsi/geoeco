import json

filename = 'usagetypes-day-cost_2024-02-01_2024-02-26.csv'
f = open(filename, 'r')
with f:
    answer = f.read()
answer = answer.replace('"', '').split('\n')
dates = []
result = list()
data = answer[0].split(',')
for i in range(1, len(data)):
    dates.append(data[i])
for j in range(1, len(answer)):
    data = answer[j].split(',')
    unit = {'scu': data[0]}
    param = dict()
    for i, date in enumerate(dates):
        param[date] = data[i+1]
    unit['values'] = param
    result.append(unit)
print(json.dumps(result, indent=4))

# запись в БД
sql_query = ''
count = 0
for data in result:
    for key in data['values']:
        sql_text = "insert into ... (scu, date, cost, service) values ('" + data['scu'] + "', " + "'" + key + "', " + \
                   data['values'][key] + ", 'qwak'"
        sql_text += ');\n'
        sql_query += sql_text
        count += 1

print(sql_query)
print(count)