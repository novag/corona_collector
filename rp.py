#!/usr/bin/env python3

import influxdb
import io
import json
import locale
import os
import requests
import sys
import traceback
from datetime import datetime
from lxml import html


DEBUG = False

STATE = 'Rheinland-Pfalz'
STATE_SHORT = 'RP'

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

    def _normalize_county(self, county, is_city):
        if is_city:
            county = county.replace('KS ', '')

            county = county.replace('Frankenthal', 'Frankenthal (Pfalz)')
            county = county.replace('Landau i.d. Pfalz', 'Landau in der Pfalz')
            county = county.replace('Landau i.d.Pfalz', 'Landau in der Pfalz')
            county = county.replace('Ludwigshafen', 'Ludwigshafen am Rhein')
            county = county.replace('Neustadt Weinst.', 'Neustadt an der Weinstraße')

            county = '{} (Stadt)'.format(county)
        else:
            county = county.replace('LK ', '')

            county = county.replace('Altenkirchen', 'Altenkirchen (Westerwald)')
            county = county.replace('Bitburg-Prüm', 'Eifelkreis Bitburg-Prüm')
            county = county.replace('Rhein-Hunsrück', 'Rhein-Hunsrück-Kreis')
            county = county.replace('Südliche Weinstr.', 'Südliche Weinstraße')

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
        dt_str = self.tree.xpath('//table/tbody/tr[last()]/td/text()')[0]
        dt = datetime.strptime(dt_str, 'Stand: %d.%m.%Y; %H Uhr').strftime('%Y-%m-%dT%H:%M:%SZ')

        rows = self.tree.xpath('//table/tbody/tr')

        if rows[0].xpath('td/descendant-or-self::*/text()')[0] != 'Landkreis':
            raise Exception('ERROR: Landkreis table not found')

        data = []
        infected_sum = 0
        death_sum = 0
        is_city = False
        for row in rows[1:-1]:
            cells = row.xpath('td/descendant-or-self::*/text()')

            if not cells:
                continue

            if cells[0].strip() == 'Stadt':
                is_city = True
                continue

            if cells[0].strip() == 'Stand:':
                continue

            county = self._normalize_county(cells[0].replace('\xa0', '').strip(), is_city)
            infected_str = cells[1].replace('\xa0', '').strip()
            death_str = cells[2].strip()

            infected = int(infected_str)
            infected_sum += infected

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


data_url = 'https://msagd.rlp.de/de/unsere-themen/gesundheit-und-pflege/gesundheitliche-versorgung/oeffentlicher-gesundheitsdienst-hygiene-und-infektionsschutz/infektionsschutz/informationen-zum-coronavirus-sars-cov-2/'
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
