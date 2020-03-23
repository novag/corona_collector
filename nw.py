#!/usr/bin/env python3

import influxdb
import io
import json
import os
import locale
import re
import requests
import sys
import traceback
from datetime import datetime
from lxml import html


DEBUG = False

STATE = 'Nordrhein-Westfalen'
STATE_SHORT = 'NW'

BASE_PATH = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(BASE_PATH, 'config.json')
POPULATION_PATH = os.path.join(BASE_PATH, 'population.json')

with open(CONFIG_PATH) as file:
    CONFIG = json.load(file)

with open(POPULATION_PATH, encoding='utf-8') as file:
    POPULATION = json.load(file)


def notify(msg):
    print(msg)

    if 'pushover' not in CONFIG:
        return

    requests.post(
        'https://api.pushover.net/1/messages.json',
        json={
            'token': CONFIG['pushover']['token'],
            'user': CONFIG['pushover']['user'],
            'title': 'Corona {}: Fehler!'.format(STATE_SHORT),
            'message': msg
        }
    )


class CoronaParser:
    def __init__(self, db, tree):
        self.db = db
        self.tree = tree

        locale.setlocale(locale.LC_TIME, "de_DE.utf8")

    def _store(self, data):
        if DEBUG:
            print(data)
            return

        try:
            if not self.db.write_points(data):
                raise Exception('ERROR: CoronaParser: _store: false')
        except influxdb.exceptions.InfluxDBServerError as e:
            raise Exception('ERROR: CoronaParser: _store: {}'.format(e))
        except influxdb.exceptions.InfluxDBClientError as e:
            raise Exception('ERROR: CoronaParser: _store: {}'.format(e))

    def _raw_county(self, county):
        county = county.replace(' (Kreis)', '')

        county = county.replace('Mülheim / Ruhr', 'Mülheim an der Ruhr')

        return county

    def _calculate_p10k(self, county, infected):
        county_raw = self._raw_county(county)

        try:
            if county == 'Aachen & Städteregion Aachen':
                population = POPULATION['city'][STATE_SHORT]['Aachen']
                population += POPULATION['county'][STATE_SHORT]['Aachen']
            elif 'Kreis' in county or county == 'Höxter':
                population = POPULATION['county'][STATE_SHORT][county_raw]
            else:
                population = POPULATION['city'][STATE_SHORT][county_raw]

            return round(infected * 10000 / population, 2)
        except:
            notify('{}/{} not found in population database.'.format(county, county_raw))

        return None

    def _calculate_p100k(self, infected):
        population = POPULATION['state'][STATE_SHORT]

        return round(infected * 100000 / population, 2)

    def parse(self):
        dt_text = self.tree.xpath('//div[@class="group-introduction field-group-div"]//div[@class="field-item even"]/p/text()')[1]
        dt_text = dt_text.split('Stand: ')[-1]
        dt = datetime.strptime(dt_text, '%d. %B %Y, %H.%M Uhr. ').strftime('%Y-%m-%dT%H:%M:%SZ')

        counties_table = self.tree.xpath('//table')[0]

        if counties_table.xpath('thead/tr/th/text()')[0] != 'Landkreis/ kreisfreie Stadt':
            raise Exception('ERROR: Landkreis table not found')

        death_str = counties_table.xpath('parent::div/p/text()')[0].strip()
        result = re.findall(r'(\d+) Todesfälle', death_str)
        if not result:
            raise ValueError('ERROR: CoronaParser: death count not found')

        death_sum = int(result[0])

        # Counties
        data = []
        infected_sum = 0
        for row in counties_table.xpath('tbody/tr'):
            cells = row.xpath('td/text()')

            if not cells:
                continue

            county = cells[0].strip()
            infected_str = cells[1].strip()

            try:
                infected = int(infected_str)
            except ValueError:
                infected = 0
            infected_sum += infected

            data.append({
                'measurement': 'infected_de_state',
                'tags': {
                    'state': STATE,
                    'county': county
                },
                'time': dt,
                'fields': {
                    'count': infected,
                    'p10k': self._calculate_p10k(county, infected)
                }
            })

        data.append({
            'measurement': 'infected_de',
            'tags': {
                'state': STATE
            },
            'time': dt,
            'fields': {
                'count': infected_sum,
                'p100k': self._calculate_p100k(infected_sum),
                'death': death_sum
            }
        })

        self._store(data)

        return dt


data_url = 'https://www.mags.nrw/coronavirus-fallzahlen-nrw'
if len(sys.argv) == 2:
    data_url = sys.argv[1]

r = requests.get(data_url, headers={'User-Agent': CONFIG['user_agent']})
if not r.ok:
    print('ERROR: failed to fetch data, status code: {}'.format(r.stats_code))
    sys.exit(1)

if DEBUG:
    db_client = None
else:
    db_client = influxdb.InfluxDBClient(
        host=CONFIG['db']['host'],
        port=CONFIG['db']['port'],
        username=CONFIG['db']['username'],
        password=CONFIG['db']['password']
    )
    db_client.switch_database(CONFIG['db']['database'])

corona_parser = CoronaParser(db_client, html.fromstring(r.text))

try:
    dt = corona_parser.parse()
    print('Data updated at {}'.format(dt))
except Exception as e:
    traceback.print_exc()
    notify(str(e))

if not DEBUG:
    db_client.close()
