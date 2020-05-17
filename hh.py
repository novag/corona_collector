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

STATE = 'Hamburg'
STATE_SHORT = 'HH'

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

    def _calculate_state_per_population(self, infected, per_population):
        population = POPULATION['state'][STATE_SHORT]

        return round(infected * per_population / population, 2)

    def _calculate_state_p10k(self, infected):
        return self._calculate_state_per_population(infected, 10000)

    def _calculate_state_p100k(self, infected):
        return self._calculate_state_per_population(infected, 100000)

    def parse(self):
        dt_text = self.tree.xpath('//p[@class="article-date"]/text()')[0]
        try:
            dt = datetime.strptime(dt_text, ' %d. %B %Y %H:%M\xa0Uhr').strftime('%Y-%m-%dT%H:%M:%SZ')
        except:
            dt = datetime.strptime(dt_text, ' %d. %B %Y').replace(hour=13).strftime('%Y-%m-%dT%H:%M:%SZ')

        paragraph = ' '.join(self.tree.xpath('//div[@class="richtext"]/p//text()'))

        infected_matches = re.findall(r'insgesamt ([\d\.]+) angestiegen', paragraph)
        if not infected_matches:
            infected_matches = re.findall(r'(?:insgesamt|liegt nun|damit) bei +([\d\.]+)', paragraph)
            if not infected_matches:
                raise ValueError('ERROR: CoronaParser: infected number not found')

        death_matches = re.findall(r'([\d\.]+) Personen mit einer COVID-19-Infektion verstorben', paragraph)
        if not death_matches:
            death_matches = re.findall(r'([\d\.]+) +Personen die COVID-19-Infektion als todesursächlich', paragraph)
            if not death_matches:
                raise ValueError('ERROR: CoronaParser: death number not found')

        infected_str = infected_matches[0].replace('.', '')
        infected_sum = int(infected_str)

        death_str = death_matches[0].replace('.', '')
        death_sum = int(death_str)

        data = [{
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
        }]

        self._store(data)

        return dt


search_url = 'https://www.hamburg.de/coronavirus/pressemeldungen/'

if len(sys.argv) == 2:
    data_url = sys.argv[1]
else:
    r = requests.get(search_url, headers={'User-Agent': CONFIG['user_agent']})
    if not r.ok:
        print('ERROR: failed to fetch posts, status code: {}'.format(r.stats_code))
        sys.exit(1)

    tree = html.fromstring(r.text)
    posts = tree.xpath('//div[@class="container-border"]/div[@class="row row-eq-height"]/div[@class="col-xs-12 col-md-12 teaser teaser-thumb teaser-thumb-fhh "]')
    data_url = None
    for post in posts:
        a = post.xpath('.//a')[0]
        title = a.xpath('h3/span[contains(@class, "teaser-headline")]/text()')[0].strip()
        href = a.xpath('@href')[0]

        if title == 'Informationen zum aktuellen Stand COVID-19 in Hamburg' or 'Coronavirus: ' in title:
            data_url = href
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
