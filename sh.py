#!/usr/bin/env python3

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

STATE = 'Schleswig-Holstein'
STATE_SHORT = 'SH'

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
        return county

    def _calculate_p10k(self, county, infected):
        try:
            if county in POPULATION['county'][STATE_SHORT]:
                population = POPULATION['county'][STATE_SHORT][county]
            else:
                population = POPULATION['city'][STATE_SHORT][county]

            return round(infected * 10000 / population, 2)
        except:
            notify('{} not found in population database.'.format(county))

        return None

    def _calculate_state_p10k(self, infected):
        population = POPULATION['state'][STATE_SHORT]

        return round(infected * 10000 / population, 2)

    def _calculate_state_p100k(self, infected):
        population = POPULATION['state'][STATE_SHORT]

        return round(infected * 100000 / population, 2)

    def parse(self):
        dt_text = ' '.join(self.tree.xpath('//div[@class="singleview"]/div[@class="teaserText"]/p/strong/text()')[:2])
        dt = datetime.strptime(dt_text, 'Datenstand %d.%m.%Y %H Uhr').strftime('%Y-%m-%dT%H:%M:%SZ')

        body_paragraph = self.tree.xpath('//div[@class="bodyText"]/p/strong/text()')

        if body_paragraph[0] != 'Gemeldete Fälle':
            raise ValueError('ERROR: infected count not found')

        infected_str = body_paragraph[1].replace('.', '')
        infected_sum = int(infected_str)

        if not body_paragraph[5].startswith('Todesfälle'):
            raise ValueError('ERROR: infected count not found')

        death_str = body_paragraph[5].split(': ')[1].strip().replace('.', '')
        death_sum = int(death_str)

        data = [{
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
        }]

        self._store(data)

        return dt


data_url = 'https://www.schleswig-holstein.de/DE/Schwerpunkte/Coronavirus/Zahlen/zahlen_node.html'
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
