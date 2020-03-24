#!/usr/bin/env python3

import influxdb
import json
import os
import sys

BASE_PATH = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(BASE_PATH, 'config.json')

with open(CONFIG_PATH) as file:
    CONFIG = json.load(file)

db = influxdb.InfluxDBClient(
    host=CONFIG['db']['host'],
    port=CONFIG['db']['port'],
    username=CONFIG['db']['username'],
    password=CONFIG['db']['password']
)
db.switch_database(CONFIG['db']['database'])

old_county = sys.argv[1]
county = sys.argv[2]

print('Start {} -> {}'.format(old_county, county))

result = db.query("""SELECT * FROM "infected_de_state" WHERE county = '{}'""".format(old_county)).items()[0][1]
result = list(result)

state = result[0]['state']
data = []
for row in result:
    if row['state'] != state:
        print('FAIL: multiple states for county: county={}, state={}'.format(row['county'], ','.join(state, row['state'])))
        data = None
        break

    if row['p10k']:
        p10k = float(row['p10k'])
    else:
        p10k = None
    data.append({
        'measurement': 'infected_de_state',
        'tags': {
            'state': state,
            'county': county
        },
        'time': row['time'],
        'fields': {
            'count': row['count'],
            'p10k': p10k,
            'death': row['death']
        }
    })

if db.write_points(data):
    db.query("""DROP SERIES FROM "infected_de_state" WHERE county = '{}'""".format(old_county))
else:
    print('FAIL {} -> {}'.format(old_county, county))

print('End {} -> {}'.format(old_county, county))

db.close()
