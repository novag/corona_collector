#!/usr/bin/env python3

import influxdb
import io
import json
import locale
import os
import re
import requests
import sys
import traceback
from datetime import datetime
from lxml import html


DEBUG = False

STATE = 'Thüringen'
STATE_SHORT = 'TH'

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

    def _normalize_county(self, county):
        return county

    def _calculate_p10k(self, county, infected):
        try:
            if county in POPULATION['city'][STATE_SHORT]:
                population = POPULATION['city'][STATE_SHORT][county]
            else:
                population = POPULATION['county'][STATE_SHORT][county]

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
        dt_array = self.tree.xpath('//section[contains(@class, "th-box")]//div[@class="frame frame-default frame-type-text frame-layout-0"]/*[self::h2 or self::h3]/text()')
        for dt_text in dt_array:
            if 'Stand: ' in dt_text:
                result = re.findall(r'(\(Stand: .+?\))', dt_text)
                break

        if not result:
            raise ValueError('ERROR: dt text not found')
        dt_text = result[0]

        try:
            dt = datetime.strptime(dt_text, '(Stand: %d. %B %Y, %H Uhr)').strftime('%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            dt = datetime.strptime(dt_text, '(Stand: %d. %B %Y)').replace(hour=12).strftime('%Y-%m-%dT%H:%M:%SZ')

        table = self.tree.xpath('//table[@class="table table-striped"]')[0]

        if table.xpath('thead/tr/th[3]/text()')[0] != 'aktueller':
            raise Exception('ERROR: table not found')

        # Counties
        data = []
        infected_sum = 0
        death_sum = 0
        for row in table.xpath('tbody/tr'):
            thcell = row.xpath('th/text()')
            cells = row.xpath('td')

            if not thcell or not cells:
                continue

            if len(cells) != 7:
                raise Exception('ERROR: invalid cell length: {}'.format(len(cells)))

            if thcell[0].strip() == 'Summe':
                continue

            county = self._normalize_county(thcell[0].strip())
            infected_str = cells[1].xpath('text()')[0].strip()
            death_str = cells[6].xpath('text()')[0].strip()

            if infected_str:
                infected = int(infected_str)
                infected_sum += infected
            else:
                infected = 0

            if death_str:
                death = int(death_str)
                death_sum += death
            else:
                death = 0

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


data_url = 'https://www.landesregierung-thueringen.de/corona-bulletin/'
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
