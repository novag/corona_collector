#!/usr/bin/env python3

import csv
import influxdb
import json
import os
import sys

BASE_PATH = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(BASE_PATH, 'config.json')
STATES_CSV_PATH = os.path.join(BASE_PATH, 'states.csv')

POPULATION = {}

with open(CONFIG_PATH) as file:
    CONFIG = json.load(file)

with open(STATES_CSV_PATH, encoding='utf-8') as file:
    dict_reader = csv.DictReader(file)
    for row in dict_reader:
        state = row['state']
        population = int(row['population'])

        POPULATION[state] = population

db = influxdb.InfluxDBClient(
    host=CONFIG['db']['host'],
    port=CONFIG['db']['port'],
    username=CONFIG['db']['username'],
    password=CONFIG['db']['password']
)
db.switch_database(CONFIG['db']['database'])


def calculate_p100k( state, infected):
    population = POPULATION[state]

    return round(infected * 100000 / population, 2)

result = db.query('SELECT * FROM "infected_de" WHERE time >= {}'.format(sys.argv(1))).items()[0][1]
rows = list(result)

data = []
for row in rows:
    data.append({
        'measurement': 'infected_de',
        'tags': {
            'state': row['state']
        },
        'time': row['time'],
        'fields': {
            'count': row['count'],
            'p100k': calculate_p100k(row['state'], row['count'])
        }
    })

if not db.write_points(data):
    print('FAIL -> {}'.format(data))

print('Done.')

db.close()