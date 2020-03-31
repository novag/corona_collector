#!/usr/bin/env python3

import csv
import influxdb
import io
import json
import os
import requests
import sys
import traceback
from datetime import datetime
from lxml import html


DEBUG = False

STATE = 'Niedersachsen'
STATE_SHORT = 'NI'

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
    def __init__(self, db, html_text, csv_content):
        self.db = db
        self.tree = html.fromstring(html_text)
        self.data = list(csv.DictReader(csv_content.decode('utf-8').splitlines(), delimiter=';'))

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

    def _normalize_county(self, county):
        county = county.replace('Nienburg (Weser)', 'Nienburg/Weser')
        county = county.replace('LK ', '')

        if county.startswith('SK '):
            county = county.replace('SK ', '')
            county = '{} (Stadt)'.format(county)

        return county

    def _calculate_p10k(self, county, infected):
        try:
            if county == 'Region Hannover':
                population = POPULATION['city'][STATE_SHORT]['Hannover']
                population += POPULATION['county'][STATE_SHORT]['Hannover']
            elif county.endswith('(Stadt)'):
                raw_county = county.replace(' (Stadt)', '')
                population = POPULATION['city'][STATE_SHORT][raw_county]
            else:
                population = POPULATION['county'][STATE_SHORT][county]

            return round(infected * 10000 / population, 2)
        except:
            raise Exception('{} not found in population database.'.format(county))

        return None

    def _calculate_state_p10k(self, infected):
        population = POPULATION['state'][STATE_SHORT]

        return round(infected * 10000 / population, 2)

    def _calculate_state_p100k(self, infected):
        population = POPULATION['state'][STATE_SHORT]

        return round(infected * 100000 / population, 2)

    def parse(self):
        dt_text = self.tree.xpath('//p/b/text()')[2]
        dt = datetime.strptime(dt_text, 'Datenstand: %d.%m.%Y %H:%M Uhr').strftime('%Y-%m-%dT%H:%M:%SZ')

        # Counties
        data = []
        infected_sum = 0
        death_sum = 0
        for row in self.data:
            county = self._normalize_county(row['Landkreis'].strip())
            infected_str = row['bestätigte Fälle'].strip()
            death_str = row['verstorbene Fälle'].strip()

            infected = int(infected_str)
            infected_sum += infected

            death = int(death_str)
            death_sum += death

            data.append({
                'measurement': 'infected_de_state',
                'tags': {
                    'state': STATE,
                    'county': county
                },
                'time': dt,
                'fields': {
                    'count': infected,
                    'p10k': self._calculate_p10k(county, infected),
                    'death': death
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
                'p10k': self._calculate_state_p10k(infected_sum),
                'p100k': self._calculate_state_p100k(infected_sum),
                'death': death_sum
            }
        })

        self._store(data)

        return dt


data_url = 'https://www.apps.nlga.niedersachsen.de/corona/iframe.php'
if len(sys.argv) == 3:
    data_url = sys.argv[1]

r_web = requests.get(data_url, headers={'User-Agent': CONFIG['user_agent']})
if not r_web.ok:
    print('ERROR: failed to fetch data, status code: {}'.format(r.status_code))
    sys.exit(1)

csv_url = 'https://www.apps.nlga.niedersachsen.de/corona/download.php?csv-file'
if len(sys.argv) == 3:
    csv_url = sys.argv[2]

r_csv = requests.get(csv_url, headers={'User-Agent': CONFIG['user_agent']})
if not r_csv.ok:
    print('ERROR: failed to fetch csv, status code: {}'.format(r_csv.status_code))
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

corona_parser = CoronaParser(db_client, r_web.text, r_csv.content)

try:
    dt = corona_parser.parse()
    print('Data updated at {}'.format(dt))
except Exception as e:
    traceback.print_exc()
    notify(str(e))

if not DEBUG:
    db_client.close()
