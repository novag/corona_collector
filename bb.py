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

STATE = 'Brandenburg'
STATE_SHORT = 'BB'

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
        county = county.replace('Brandenburg a. d. H.', 'Brandenburg an der Havel')
        county = county.replace('Brandenburg a. d. Havel', 'Brandenburg an der Havel')

        return county

    def _calculate_per_population(self, county, infected, per_population):
        try:
            if county in POPULATION['city'][STATE_SHORT]:
                population = POPULATION['city'][STATE_SHORT][county]
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
        table = self.tree.xpath('//table/tbody/tr')

        dt_text = table[0].xpath('td')[2].xpath('p/strong/text()')[-1].replace(' ', '')
        dt = datetime.strptime(dt_text, 'Stand:%d.%m.,%H:%MUhr').replace(year=2020).strftime('%Y-%m-%dT%H:%M:%SZ')

        # Counties
        data = []
        infected_sum = 0
        death_sum = 0
        for row in table[1:]:
            cells = row.xpath('td')

            if not cells:
                continue

            if cells[0].xpath('p/strong')[0].text.strip() == 'Brandenburg gesamt':
                continue

            county = self._normalize_county(cells[0].xpath('p/strong')[0].text.strip())
            infected_str = cells[2].xpath('p')[0].text.replace('*', '').strip()
            death_str = cells[-1].xpath('p')[0].text.replace('*', '').strip()

            if infected_str == '---':
                infected = 0
            else:
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


search_url = 'https://msgiv.brandenburg.de/msgiv/de/presse/pressemitteilungen/'

if len(sys.argv) == 2:
    data_url = sys.argv[1]
else:
    r = requests.get(search_url, headers={'User-Agent': CONFIG['user_agent']})
    if not r.ok:
        print('ERROR: failed to search overview, status code: {}'.format(r.stats_code))
        sys.exit(1)

    tree = html.fromstring(r.text)
    posts = tree.xpath('//form[@action="filterform"]/div[@class="trennung medium-12 "]/div[@class="bb-teaser-item"]')
    data_url = None
    for post in posts:
        title = post.xpath('h2/a/text()')[0]
        href = post.xpath('h2/a/@href')[0]

        if 'Erkrankungen an COVID-19' in title or 'COVID-19-Erkrankungen' in title or 'COVID-19-Fälle' in title:
            data_url = 'https://msgiv.brandenburg.de{}'.format(href)
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
