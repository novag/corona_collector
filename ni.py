#!/usr/bin/env python3

import influxdb
import io
import json
import os
import locale
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

    if 'pb_token' not in CONFIG:
        return

    requests.post(
        'https://api.pushbullet.com/v2/pushes',
        headers={'Access-Token': CONFIG['pb_token']},
        json={
            'type': 'note',
            'title': 'Corona {}: Fehler!'.format(STATE_SHORT),
            'body': msg
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
        county = county.replace('Nienburg (Weser)', 'Nienburg/Weser')
        county = county.replace('Rotenburg/Wümme', 'Rotenburg (Wümme)')

        return county

    def _calculate_p10k(self, county, infected, is_county):
        county_raw = self._raw_county(county)

        try:
            if county == 'Region Hannover':
                population = POPULATION['city'][STATE_SHORT]['Hannover']
                population += POPULATION['county'][STATE_SHORT]['Hannover']
            elif is_county:
                population = POPULATION['county'][STATE_SHORT][county_raw]
            else:
                population = POPULATION['city'][STATE_SHORT][county_raw]

            return round(infected * 10000 / population, 2)
        except:
            raise Exception('{}/{} not found in population database.'.format(county, county_raw))

        return None

    def parse(self):
        counties_div = self.tree.xpath('//div[@class="group complementary span1of4"]//div[@class="content"]')[0]

        dt_text = counties_div.xpath('normalize-space(p/i/text())')
        try:
            dt = datetime.strptime(dt_text, 'zuletzt aktualisiert am %d.%m.%Y, %H Uhr').strftime('%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            dt = datetime.strptime(dt_text, 'zuletzt aktualisiert am %d.%m.%Y, %H:%M Uhr').strftime('%Y-%m-%dT%H:%M:%SZ')

        elements = counties_div.xpath('p[4]/descendant-or-self::*/text()')

        if not elements[0].startswith('Alle registrierten'):
            raise Exception('ERROR: Landkreis paragraph not found')

        # Counties
        data = []
        infected_sum = 0

        infected = None
        is_county = True
        for text in elements[1:]:
            text = text.strip()

            if not text or text.startswith('('):
                continue

            if text.startswith('Hinweis') or text.startswith('Hier'):
                break

            if 'Fall i' in text or 'Fälle i' in text:
                infected = int(text.split(' ')[0])
                infected_sum += infected
                is_county = text.endswith('LK')
                continue

            if text == 'LK':
                is_county = True
                continue

            county = text.split(' (+')[0]

            data.append({
                'measurement': 'infected_de_state',
                'tags': {
                    'state': STATE,
                    'county': county
                },
                'time': dt,
                'fields': {
                    'count': infected,
                    'p10k': self._calculate_p10k(county, infected, is_county)
                }
            })

        data.append({
            'measurement': 'infected_de',
            'tags': {
                'state': STATE
            },
            'time': dt,
            'fields': {
                'count': infected_sum
            }
        })

        self._store(data)

        return dt


data_url = 'https://www.niedersachsen.de/Coronavirus'
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
