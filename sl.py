#!/usr/bin/env python3

import influxdb
import io
import json
import os
import re
import requests
import sys
import traceback
from datetime import datetime
from lxml import html


DEBUG = False

STATE = 'Saarland'
STATE_SHORT = 'SL'

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
    def __init__(self, db, html_text):
        self.db = db
        self.tree = html.fromstring(html_text)

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

    def _calculate_state_per_population(self, infected, per_population):
        population = POPULATION['state'][STATE_SHORT]

        return round(infected * per_population / population, 2)

    def _calculate_state_p10k(self, infected):
        return self._calculate_state_per_population(infected, 10000)

    def _calculate_state_p100k(self, infected):
        return self._calculate_state_per_population(infected, 100000)

    def parse(self):
        dt_text = self.tree.xpath('//main[@class="main row"]/div/p/strong/text()')[0].strip().replace('–', '-')
        try:
            dt = datetime.strptime(dt_text, '%d.%m.%Y - %H:%M Uhr').strftime('%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            dt = datetime.strptime(dt_text, '%d.%m.%Y - %H Uhr').strftime('%Y-%m-%dT%H:%M:%SZ')

        message = ' '.join(self.tree.xpath('//main[@class="main row"]/div/p/text()'))

        infected_matches = re.findall(r'infizierten Personen beträgt landesweit ([\d\.]+)', message)
        if not infected_matches:
            raise ValueError('ERROR: CoronaParser: infected count not found')

        death_matches = re.findall(r'Zahl der Verstorbenen: ([\d\.]+)', message)
        if not death_matches:
            raise ValueError('ERROR: CoronaParser: death count not found')

        infected_sum = int(infected_matches[0].replace('.', ''))
        death_sum = int(death_matches[0].replace('.', ''))

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


data_url = 'https://corona.saarland.de/DE/service/chronologie-corona/chronologie-corona_node.html'
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

corona_parser = CoronaParser(db_client, r.text)

try:
    dt = corona_parser.parse()
    print('Data updated at {}'.format(dt))
except Exception as e:
    traceback.print_exc()
    notify(str(e))

if not DEBUG:
    db_client.close()
