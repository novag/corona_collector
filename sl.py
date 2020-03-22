#!/usr/bin/env python3

import influxdb
import io
import json
import os
import re
import requests
import sys
import traceback
from datetime import datetime
from lxml import html


DEBUG = False

STATE = 'Saarland'
STATE_SHORT = 'SL'

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

    def _raw_county(self, county):
        return county

    def _calculate_p10k(self, county, infected):
        county_raw = self._raw_county(county)

        try:
            population = POPULATION['county'][STATE_SHORT][county_raw]

            return round(infected * 10000 / population, 2)
        except:
            notify('{}/{} not found in population database.'.format(county, county_raw))

        return None

    def parse(self):
        message = ' '.join(self.tree.xpath('//div[@class="textchapter_frame"]/p/descendant-or-self::*/text()'))

        dt_text = self.tree.xpath('//div[@class="textchapter_teaser_frame"]/text()')[0].strip()
        dt = datetime.strptime(dt_text, 'Pressemitteilung vom %d.%m.%Y - %H:%M Uhr')

        result = re.findall(r'\((\d+) Uhr\)', message)
        if not result:
            raise ValueError('ERROR: CoronaParser: time not found')

        hour = int(result[0])
        dt = dt.replace(hour=hour, minute=0).strftime('%Y-%m-%dT%H:%M:%SZ')

        result = re.findall(r'(\d+) bestätigte Fälle', message)
        if not result:
            raise ValueError('ERROR: CoronaParser: infected count not found')

        infected = int(result[0])

        data = [{
            'measurement': 'infected_de',
            'tags': {
                'state': STATE
            },
            'time': dt,
            'fields': {
                'count': infected
            }
        }]

        self._store(data)

        return dt


search_url = 'https://www.saarland.de/7250.htm'

if len(sys.argv) == 2:
    data_url = sys.argv[1]
else:
    r = requests.get(search_url, headers={'User-Agent': CONFIG['user_agent']})
    if not r.ok:
        print('ERROR: failed to fetch posts, status code: {}'.format(r.stats_code))
        sys.exit(1)

    tree = html.fromstring(r.text)
    posts = tree.xpath('//div[@class="contentteaserlist_frame"]/div[@class="contentteaserlist_row"]')
    data_url = None
    for post in posts:
        a = post.xpath('.//h4/a')[0]
        title = a.text
        href = a.get('href')

        if title.startswith('Saarländisches Gesundheitsministerium bestätigt'):
            data_url = 'https://www.saarland.de{}'.format(href)
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
