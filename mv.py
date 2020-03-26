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

STATE = 'Mecklenburg-Vorpommern'
STATE_SHORT = 'MV'

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

    def _normalize_county(self, county):
        county = county.replace('Schwerin', 'Schwerin (Stadt)')
        county = county.replace('Hansestadt Rostock', 'Rostock (Stadt)')
        county = county.replace('Landkreis Rostock', 'Rostock')

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
        table = self.tree.xpath('//table/tr')

        dt_text = table[1].xpath('td/p/strong/text()')[2]
        try:
            dt = datetime.strptime(dt_text, 'Stand %d.%m. %H:%M Uhr').replace(year=2020).strftime('%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            try:
                dt = datetime.strptime(dt_text, 'Stand %d.%m. %H:%M').replace(year=2020).strftime('%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                dt = datetime.strptime(dt_text, 'Stand %d.%m. %H Uhr').replace(year=2020).strftime('%Y-%m-%dT%H:%M:%SZ')

        # Counties
        data = []
        infected_sum = 0
        for row in table[2:]:
            cells = row.xpath('td/p/text()')

            if not cells:
                continue

            county = self._normalize_county(cells[0].strip())
            infected_str = cells[-1].strip()

            if county == 'SUMME':
                continue

            infected = int(infected_str)
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
                'p10k': self._calculate_state_p10k(infected_sum),
                'p100k': self._calculate_state_p100k(infected_sum)
            }
        })

        self._store(data)

        return dt


search_url = 'https://www.regierung-mv.de/Landesregierung/wm/Aktuell/?sa.query=Aktueller+Stand+Corona-Infektionen&sa.pressemitteilungen.area=11&sa.month=alle&sa.year=alle&search_filter_submit='

if len(sys.argv) == 2:
    data_url = sys.argv[1]
else:
    r = requests.get(search_url, headers={'User-Agent': CONFIG['user_agent']})
    if not r.ok:
        print('ERROR: failed to search overview, status code: {}'.format(r.stats_code))
        sys.exit(1)

    tree = html.fromstring(r.text)
    path = tree.xpath('//div[@class="resultlist"]/div[contains(@class, "teaser")]//a/@href')[0]
    data_url = 'https://www.regierung-mv.de{}'.format(path)

r = requests.get(data_url, headers={'User-Agent': CONFIG['user_agent']})
if not r.ok:
    print('ERROR: failed to search overview, status code: {}'.format(r.stats_code))
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
