#!/usr/bin/env python3

import influxdb
import json
import os
import sys

BASE_PATH = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(BASE_PATH, 'config.json')

with open(CONFIG_PATH) as file:
    CONFIG = json.load(file)

with open(sys.argv[1], encoding='utf-8') as file:
    LOOKUP = json.load(file)

db = influxdb.InfluxDBClient(
    host=CONFIG['db']['host'],
    port=CONFIG['db']['port'],
    username=CONFIG['db']['username'],
    password=CONFIG['db']['password']
)
db.switch_database(CONFIG['db']['database'])

for old_county_name in LOOKUP:
    print('Start {} -> {}'.format(old_county_name, LOOKUP[old_county_name]))

    result = db.query("""SELECT * FROM "infected_de_state" WHERE county = '{}'""".format(old_county_name)).items()[0][1]
    rows = list(result)

    jdata = []
    for row in rows:
        entry = {
            'measurement': 'infected_de_state',
            'tags': {
                'state': row['state'],
                'county': LOOKUP[old_county_name]
            },
            'time': row['time'],
            'fields': {
                'count': row['count']
            }
        }

        if row['p10k']:
            entry['fields']['p10k'] = row['p10k']

        jdata.append(entry)

    if db.write_points(jdata):
        db.query("""DROP SERIES FROM "infected_de_state" WHERE county = '{}'""".format(old_county_name))
    else:
        print('FAIL {} -> {}'.format(old_county_name, LOOKUP[old_county_name]))

    print('End {} -> {}'.format(old_county_name, LOOKUP[old_county_name]))

db.close()