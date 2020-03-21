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
        county = county.replace('Stadtgemeinde ', '')

        return county

    def _calculate_p10k(self, county, infected):
        county_raw = self._raw_county(county)

        try:
            population = POPULATION['city'][STATE_SHORT][county_raw]

            return round(infected * 10000 / population, 2)
        except:
            notify('{}/{} not found in population database.'.format(county, county_raw))

        return None

    def parse(self):
        dt_text = self.tree.xpath('//span[@class="article_time"]/text()')[0]
        dt = datetime.strptime(dt_text, '%d.%m.%Y').replace(hour=12).strftime('%Y-%m-%dT%H:%M:%SZ')

        rows = self.tree.xpath('//table/tr')

        if rows[0].xpath('th/text()')[0] != 'Bestätigte Fälle insgesamt':
            raise Exception('ERROR: table not found')

        # Counties
        data = []
        infected_sum = 0
        for row in rows[1:]:
            thcell = row.xpath('th/text()')
            cells = row.xpath('td/text()')

            if not thcell or not cells:
                continue

            county = thcell[0].strip()
            infected_str = cells[0].strip()

            if county == 'Land Bremen':
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
                'count': infected_sum
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

        if title.startswith('Aktueller Stand Corona in Bremen'):
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
