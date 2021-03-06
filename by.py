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

STATE = 'Bayern'
STATE_SHORT = 'BY'

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
        county = county.replace('Bad Tölz', 'Bad Tölz-Wolfratshausen')
        county = county.replace('Dillingen a.d. Donau', 'Dillingen an der Donau')
        county = county.replace('Mühldorf a.Inn', 'Mühldorf am Inn')
        county = county.replace('Neumarkt i.d.Opf.', 'Neumarkt in der Oberpfalz')
        county = county.replace('Neustadt a.d. Aisch-Bad Windsheim', 'Neustadt an der Aisch-Bad Windsheim')
        county = county.replace('Neustadt a.d. Waldnaab', 'Neustadt an der Waldnaab')
        county = county.replace('Pfaffenhofen a.d.Ilm', 'Pfaffenhofen an der Ilm')
        county = county.replace('Wunsiedel i.Fichtelgebirge', 'Wunsiedel im Fichtelgebirge')

        county = county.replace(' Stadt', ' (Stadt)')
        county = county.replace('Kempten', 'Kempten (Allgäu)')
        county = county.replace('Weiden', 'Weiden in der Oberpfalz')

        return county

    def _calculate_per_population(self, county, infected, per_population):
        try:
            if county.endswith('(Stadt)'):
                raw_county = county.replace(' (Stadt)', '')
                population = POPULATION['city'][STATE_SHORT][raw_county]
            else:
                population = POPULATION['county'][STATE_SHORT][county]

            return round(infected * per_population / population, 2)
        except:
            notify('{} not found in population database.'.format(county))

        return None

    def _calculate_p10k(self, county, infected):
        return self._calculate_per_population(county, infected, 10000)

    def _calculate_p100k(self, county, infected):
        return self._calculate_per_population(county, infected, 100000)

    def _calculate_state_per_population(self, infected, per_population):
        population = POPULATION['state'][STATE_SHORT]

        return round(infected * per_population / population, 2)

    def _calculate_state_p10k(self, infected):
        return self._calculate_state_per_population(infected, 10000)

    def _calculate_state_p100k(self, infected):
        return self._calculate_state_per_population(infected, 100000)

    def parse(self):
        d_text = self.tree.xpath('//div[@id="content_1c"]/script/text()')[0].split('= "')[1][:-1]
        t_text = self.tree.xpath('//p[@class="bildunterschrift"]/strong/text()')[2]
        dt = datetime.strptime(d_text + t_text, '%d.%m.%Y, %H:%M Uhr').strftime('%Y-%m-%dT%H:%M:%SZ')

        counties_table = self.tree.xpath('//div[@class="row abstand_unten"]//table')[0]

        if counties_table.xpath('tr')[0].xpath('th/span/text()')[0] != 'Landkreis/Stadt':
            raise Exception('ERROR: Landkreis table not found')

        # Counties
        data = []
        infected_sum = 0
        death_sum = 0
        for row in counties_table.xpath('tr'):
            cells = row.xpath('td/text()')

            if not cells:
                continue

            if len(cells) != 8:
                raise Exception('ERROR: invalid cells length: {}'.format(len(cells)))

            if cells[0] == 'Gesamtergebnis':
                break

            county = self._normalize_county(cells[0].strip())
            infected_str = cells[1].replace('.', '').strip()
            death_str = cells[6].replace('.', '').strip()

            infected = int(infected_str)
            infected_sum += infected

            if death_str == '-':
                death = 0
            else:
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
                    'p100k': self._calculate_p100k(county, infected),
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


data_url = 'https://www.lgl.bayern.de/gesundheit/infektionsschutz/infektionskrankheiten_a_z/coronavirus/karte_coronavirus/index.htm'
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
