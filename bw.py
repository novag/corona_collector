#!/usr/bin/env python3

import influxdb
import io
import json
import os
import openpyxl
import requests
import sys
import traceback
from datetime import datetime


DEBUG = False

STATE = 'Baden-Württemberg'
STATE_SHORT = 'BW'

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
    def __init__(self, db, doc_bytes):
        self.db = db
        self.wb = openpyxl.load_workbook(doc_bytes)
        self.ws = self.wb['Fälle Coronavirus in BW']

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

    def _parse_datetime(self, cell):
        self.dt = datetime.strptime(cell.value, 'Stand: %d.%m.%Y, %H:%M Uhr').strftime('%Y-%m-%dT%H:%M:%SZ')

    def _raw_county(self, county):
        return county.replace(' (Stadtkreis)', '')

    def _city_fix(self, county):
        return county == 'Stuttgart'

    def _calculate_p10k(self, county, infected):
        county_raw = self._raw_county(county)

        try:
            if county.endswith('(Stadtkreis)') or self._city_fix(county):
                population = POPULATION['city'][STATE_SHORT][county_raw]
            else:
                population = POPULATION['county'][STATE_SHORT][county_raw]

            return round(infected * 10000 / population, 2)
        except:
            notify('{}/{} not found in population database.'.format(county, county_raw))

        return None

    def parse(self):
        dt_row = 0
        county_row = 0

        for row in self.ws.iter_rows(min_col=self.ws.min_column, min_row=self.ws.min_row, max_row=self.ws.max_row, max_col=self.ws.min_column):
            cell_a = row[0]

            if not cell_a.value:
                continue

            if cell_a.value.startswith('Stand:'):
                dt_row = cell_a.row
            elif cell_a.value.startswith('Stadt-/Landkreis'):
                county_row = cell_a.row + 1
                break

        self._parse_datetime(self.ws['A'][dt_row - 1])

        data = []
        infected_sum = 0
        for row in self.ws.iter_rows(min_col=self.ws.min_column, min_row=county_row, max_row=self.ws.max_row, max_col=self.ws.min_column + 1):
            county = row[0].value
            infected_str = row[1].value

            if not county or not infected_str:
                continue

            if county == 'Summe':
                break

            infected = int(infected_str)
            infected_sum += infected

            data.append({
                'measurement': 'infected_de_state',
                'tags': {
                    'state': STATE,
                    'county': county
                },
                'time': self.dt,
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
            'time': self.dt,
            'fields': {
                'count': infected_sum
            }
        })

        self._store(data)

        return self.dt


data_url = 'https://sozialministerium.baden-wuerttemberg.de/fileadmin/redaktion/m-sm/intern/downloads/Downloads_Gesundheitsschutz/Tabelle_Coronavirus-Faelle-BW.xlsx'
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

corona_parser = CoronaParser(db_client, io.BytesIO(r.content))

try:
    dt = corona_parser.parse()
    print('Data updated at {}'.format(dt))
except Exception as e:
    traceback.print_exc()
    notify(str(e))

if not DEBUG:
    db_client.close()
