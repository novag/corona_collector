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
        county = county.replace('Altenkirchen', 'Altenkirchen (Westerwald)')
        county = county.replace('Bitburg-Prüm', 'Eifelkreis Bitburg-Prüm')
        county = county.replace('Rhein-Hunsrück', 'Rhein-Hunsrück-Kreis')
        county = county.replace('Südliche Weinstr.', 'Südliche Weinstraße')
        county = county.replace('Frankenthal', 'Frankenthal (Pfalz)')
        county = county.replace('Landau i.d.Pfalz', 'Landau in der Pfalz')
        county = county.replace('Ludwigshafen', 'Ludwigshafen am Rhein')
        county = county.replace('Neustadt Weinst.', 'Neustadt an der Weinstraße')

        return county

    def _calculate_p10k(self, county, infected, is_city):
        county_raw = self._raw_county(county)

        try:
            if is_city:
                population = POPULATION['city'][STATE_SHORT][county_raw]
            else:
                population = POPULATION['county'][STATE_SHORT][county_raw]

            return round(infected * 10000 / population, 2)
        except:
            notify('{}/{} not found in population database.'.format(county, county_raw))

        return None

    def parse(self):
        dt_array = self.tree.xpath('//div[@class="small-12 columns clearfix"]/p/text()')
        for string in dt_array:
            if string.endswith(' Uhr'):
                dt = datetime.strptime(string, '%d.%m. %H.%M Uhr')
                dt = dt.replace(year=2020).strftime('%Y-%m-%dT%H:%M:%SZ')
                break

        tables = self.tree.xpath('//table')
        counties_table = tables[0]
        cities_table = tables[1]

        if counties_table.xpath('.//tr')[0].xpath('.//td/p/text()')[0] != 'Landkreis':
            raise Exception('ERROR: Landkreis table not found')

        if cities_table.xpath('.//tr')[0].xpath('.//td/p/text()')[0] != 'Stadt':
            raise Exception('ERROR: Stadt table not found')

        # Counties
        data = []
        infected_sum = 0
        for row in counties_table.xpath('.//tr'):
            cells = row.xpath('.//td/p/text()')

            if not cells:
                continue

            county = cells[0].replace('\xa0', '').strip()
            infected_str = cells[1].replace('\xa0', '').strip()

            if county == 'Landkreis':
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
                    'p10k': self._calculate_p10k(county, infected, False)
                }
            })

        for row in cities_table.xpath('.//tr'):
            cells = row.xpath('.//td/p/text()')

            if not cells:
                continue

            county = cells[0].replace('\xa0', '').strip()
            infected_str = cells[1].replace('\xa0', '').strip()

            if county == 'Stadt':
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
                    'p10k': self._calculate_p10k(county, infected, True)
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
