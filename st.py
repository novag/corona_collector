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

STATE = 'Sachsen-Anhalt'
STATE_SHORT = 'ST'

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
        county = county.replace('\r\n ', '')

        if county.startswith('SK '):
            if county == 'SK Dessau':
                county = county.replace('Dessau', 'Dessau-Roßlau')
            county = county.replace('Halle', 'Halle (Saale)')
            county = county.replace('SK ', '')
            county = '{} (Stadt)'.format(county)
        elif county.startswith('LK '):
            county = county.replace('Anhalt Bitterfeld', 'Anhalt-Bitterfeld')
            county = county.replace('LK ', '')

        return county

    def _calculate_p10k(self, county, infected):
        try:
            if county.endswith('(Stadt)'):
                raw_county = county.replace(' (Stadt)', '')
                population = POPULATION['city'][STATE_SHORT][raw_county]
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
        date_text = self.tree.xpath('//span[@class="pm_datum"]/text()')[0]
        time_text = self.tree.xpath('//p[@class="MsoNormal"]/span/text()')[0]
        result = re.findall(r'(\(Stand.+\))', time_text)
        if not result:
            raise ValueError('ERROR: CoronaParser: dt text not found')

        dt_text = '{} {}'.format(date_text, result[0])
        dt = datetime.strptime(dt_text, 'Magdeburg, den %d. %B %Y (Stand %H:%M Uhr)').strftime('%Y-%m-%dT%H:%M:%SZ')

        rows = self.tree.xpath('//table/tbody/tr')

        data = []
        infected_sum = 0
        death_sum = 0
        for row in rows[1:]:
            cells = row.xpath('td')
            bigcell = cells[0].xpath('p/b/span/text()')

            if len(cells) != 4:
                raise Exception('ERROR: invalid cells length: {}'.format(len(cells)))

            if bigcell and (bigcell[0] == 'Melde-Landkreis' or bigcell[0] == 'Gesamtergebnis'):
                continue

            county = self._normalize_county(cells[0].xpath('p/span/text()')[0].strip())

            infected = int(cells[1].xpath('p/span/text()')[0].strip())
            infected_sum += infected

            death = int(cells[3].xpath('p/span/text()')[0].strip())
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


data_url = 'http://www.presse.sachsen-anhalt.de/index.php?cmd=get&id=909596&identifier=3813e08c96fabf41b28834ea1f0b8cf7'
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
