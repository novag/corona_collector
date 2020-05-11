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

STATE = 'Bremen'
STATE_SHORT = 'HB'

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
        county = county.replace('Stadtgemeinde ', '')

        return county

    def _calculate_per_population(self, county, infected, per_population):
        try:
            population = POPULATION['city'][STATE_SHORT][county]

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
        dt_text = self.tree.xpath('//span[@class="article_time"]/text()')[0]
        dt = datetime.strptime(dt_text, '%d.%m.%Y').replace(hour=10).strftime('%Y-%m-%dT%H:%M:%SZ')

        rows = self.tree.xpath('//table/tr')

        if rows[0].xpath('th/text()')[0] != 'Bestätigte Fälle insgesamt':
            raise Exception('ERROR: table not found')

        # Counties
        data = []
        infected_sum = 0
        death_sum = 0
        for row in rows[1:]:
            thcell = row.xpath('th/text()')
            cells = row.xpath('td')

            if not thcell or not cells:
                continue

            if thcell[0].strip() == 'Land Bremen':
                continue

            if len(cells) != 4:
                raise Exception('ERROR: invalid cells length: {}'.format(len(cells)))

            county = self._normalize_county(thcell[0].strip())
            infected_str = cells[0].text.split('(+')[0].strip()
            death_str = cells[3].text.split('(+')[0].strip()

            infected = int(infected_str)
            infected_sum += infected

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


search_url = 'https://www.senatspressestelle.bremen.de/list.php?template=20_pmsuche_treffer_l&query=10_pmalle_q&sv%5Bonline_date%5D%5B0%5D=%3E2020-02-20&sv%5Bfulltext%5D=corona&sv%5Bfulltext2%5D=corona&sm%5Bfulltext%5D=tablescan&sm%5Bfulltext2%5D=tablescan&sort=online_date&order=desc&suche=cor&skip=0&max=10'

if len(sys.argv) == 2:
    data_url = sys.argv[1]
else:
    r = requests.get(search_url, headers={'User-Agent': CONFIG['user_agent']})
    if not r.ok:
        print('ERROR: failed to search overview, status code: {}'.format(r.stats_code))
        sys.exit(1)

    tree = html.fromstring(r.text)
    posts = tree.xpath('//ul[@class="searchhits"]/li')
    data_url = None
    for post in posts:
        title = post.xpath('a/text()')[0].strip()
        href = post.xpath('a/@href')[0]

        if title.startswith('Aktueller Stand Corona') or title.startswith('Update Fallzahlen Corona'):
            data_url = 'https://www.senatspressestelle.bremen.de/{}'.format(href)
            break

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
