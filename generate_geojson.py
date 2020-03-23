#!/usr/bin/env python3

import csv
import influxdb
import json
import os
import sys

BASE_PATH = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(BASE_PATH, 'config.json')
GEOJSON_PATH = os.path.join(BASE_PATH, 'corona_map', 'de-counties.geojson')
GEOJSON_JS_PATH = os.path.join(BASE_PATH, 'corona_map', 'de-counties.js')

if len(sys.argv) == 2:
    GEOJSON_JS_PATH = sys.argv[1]

with open(CONFIG_PATH) as file:
    CONFIG = json.load(file)

with open(GEOJSON_PATH, encoding='utf-8') as file:
    GEOJSON = json.load(file)

db = influxdb.InfluxDBClient(
    host=CONFIG['db']['host'],
    port=CONFIG['db']['port'],
    username=CONFIG['db']['username'],
    password=CONFIG['db']['password']
)
db.switch_database(CONFIG['db']['database'])

state_result = db.query('SELECT "state", last("p10k") AS p10k FROM "infected_de" GROUP BY "state"')
county_result = db.query('SELECT "county", last("p10k") AS p10k FROM "infected_de_state" GROUP BY "county"')

states = {}

infections = {}
for row in county_result.get_points():
    infections[row['county']] = row['p10k']

for row in state_result.get_points():
    state = row['state']

    if state == 'Berlin' or state == 'Hamburg':
        infections[state] = row['p10k']

for feature in GEOJSON['features']:
    typ = feature['properties']['type']
    name = feature['properties']['name']

    if typ == 'county':
        if name == 'Hannover':
            cname = 'Region Hannover'
        elif name == 'Aachen' or name == 'Städteregion Aachen':
            cname = 'Aachen & Städteregion Aachen'
        else:
            cname = name

        try:
            feature['properties']['p10k'] = infections[cname]
        except KeyError:
            print('county: {}'.format(name))
    elif typ == 'city':
        if name == 'Aachen':
            cname = 'Aachen & Städteregion Aachen'
        else:
            cname = '{} (Stadt)'.format(name)

        try:
            if cname in infections:
                feature['properties']['p10k'] = infections[cname]
            else:
                feature['properties']['p10k'] = infections[name]
        except KeyError:
            print('city: {}'.format(name))
    else:
        print('unknown type: name={}, type={}'.format(name, typ))

with open(GEOJSON_JS_PATH, 'w', encoding='utf-8') as file:
    file.write('var counties = ')
    json.dump(GEOJSON, file, ensure_ascii=False)
    file.write(';')

print('Done.')

db.close()