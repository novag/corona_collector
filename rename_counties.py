#!/usr/bin/env python3

import influxdb
import json
import os
import sys

BASE_PATH = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(BASE_PATH, 'config.json')

with open(CONFIG_PATH) as file:
    CONFIG = json.load(file)

db = influxdb.InfluxDBClient(
    host=CONFIG['db']['host'],
    port=CONFIG['db']['port'],
    username=CONFIG['db']['username'],
    password=CONFIG['db']['password']
)
db.switch_database(CONFIG['db']['database'])

def normalize_name(state, county):
    if state == 'Brandenburg':
        county = county.replace('Brandenburg a. d. Havel', 'Brandenburg an der Havel')

        return county

    if state == 'Baden-Württemberg':
        county = county.replace('Stuttgart', 'Stuttgart (Stadt)')
        county = county.replace(' (Stadtkreis)', ' (Stadt)')

        return county

    if state == 'Bayern':
        county = county.replace('Bad Tölz', 'Bad Tölz-Wolfratshausen')
        county = county.replace('Dillingen a.d. Donau', 'Dillingen an der Donau')
        county = county.replace('Mühldorf a.Inn', 'Mühldorf am Inn')
        county = county.replace('Neumarkt i.d.Opf.', 'Neumarkt in der Oberpfalz')
        county = county.replace('Neustadt a.d. Aisch-Bad Windsheim', 'Neustadt an der Aisch-Bad Windsheim')
        county = county.replace('Neustadt a.d. Waldnaab', 'Neustadt an der Waldnaab')
        county = county.replace('Pfaffenhofen a.d.Ilm', 'Pfaffenhofen an der Ilm')
        county = county.replace('Wunsiedel i.Fichtelgebirge', 'Wunsiedel im Fichtelgebirge')

        county = county.replace(' Stadt', ' (Stadt)')
        county = county.replace('Weiden (Stadt)', 'Weiden in der Oberpfalz (Stadt)')

        return county

    if state == 'Bremen':
        county = county.replace('Stadtgemeinde ', '')

        return county

    if state == 'Hessen':
        county = county.replace('LK ', '')

        county = county.replace('SK Offenbach', 'Offenbach am Main (Stadt)')
        if county.startswith('SK '):
            county = county.replace('SK ', '')
            county = '{} (Stadt)'.format(county)

        return county

    if state == 'Mecklenburg-Vorpommern':
        county = county.replace('Schwerin', 'Schwerin (Stadt)')
        county = county.replace('Hansestadt Rostock', 'Rostock (Stadt)')
        county = county.replace('Landkreis Rostock', 'Rostock')

        return county

    if state == 'Niedersachsen':
        county = county.replace('Nienburg (Weser)', 'Nienburg/Weser')
        county = county.replace('LK ', '')

        if county.startswith('SK '):
            county = county.replace('SK ', '')
            county = '{} (Stadt)'.format(county)

        return county

    if state == 'Nordrhein-Westfalen':
        if 'Kreis' in county:
            county = county.replace(' (Kreis)', '')
        elif county != 'Aachen & Städteregion Aachen':
            county = county.replace('Mülheim / Ruhr', 'Mülheim an der Ruhr')
            county = '{} (Stadt)'.format(county)

        return county

    if state == 'Rheinland-Pfalz':
        county = county.replace('Altenkirchen', 'Altenkirchen (Westerwald)')
        county = county.replace('Bitburg-Prüm', 'Eifelkreis Bitburg-Prüm')
        county = county.replace('Rhein-Hunsrück', 'Rhein-Hunsrück-Kreis')
        county = county.replace('Südliche Weinstr.', 'Südliche Weinstraße')

        county = county.replace('Frankenthal', 'Frankenthal (Pfalz) (Stadt)')
        county = county.replace('Kaiserslautern', 'Kaiserslautern (Stadt)')
        county = county.replace('Koblenz', 'Koblenz (Stadt)')
        county = county.replace('Landau i.d.Pfalz', 'Landau in der Pfalz (Stadt)')
        county = county.replace('Ludwigshafen', 'Ludwigshafen am Rhein (Stadt)')
        county = county.replace('Mainz', 'Mainz (Stadt)')
        county = county.replace('Neustadt Weinst.', 'Neustadt an der Weinstraße (Stadt)')
        county = county.replace('Pirmasens', 'Pirmasens (Stadt)')
        county = county.replace('Speyer', 'Speyer (Stadt)')
        county = county.replace('Trier', 'Trier (Stadt)')
        county = county.replace('Worms', 'Worms (Stadt)')
        county = county.replace('Zweibrücken', 'Zweibrücken (Stadt)')

        return county

    if state == 'Sachsen':
        county = county.replace('Landkreis ', '')

        if county.startswith('Landeshauptstadt ') or county.startswith('Stadt '):
            county = county.replace('Landeshauptstadt ', '')
            county = county.replace('Stadt ', '')
            county = '{} (Stadt)'.format(county)

        return county

    if state == 'Sachsen-Anhalt':
        county = county.replace('Anhalt Bitterfeld', 'Anhalt-Bitterfeld')
        county = county.replace('LK ', '')

        if county.startswith('SK '):
            county = county.replace('Dessau', 'Dessau-Roßlau')
            county = county.replace('Halle', 'Halle (Saale)')
            county = county.replace('SK ', '')
            county = '{} (Stadt)'.format(county)

        return county

    return county

tags_result = db.query('SHOW TAG VALUES ON "corona" FROM "infected_de_state" WITH KEY = "county"').items()[0][1]
counties = [tag['value'] for tag in tags_result]

for old_county in counties:
    print('Start {} -> '.format(old_county), end='')

    result = db.query("""SELECT * FROM "infected_de_state" WHERE county = '{}'""".format(old_county)).items()[0][1]
    result = list(result)

    state = result[0]['state']
    county = normalize_name(state, old_county)

    print(county)

    if old_county == county:
        print('SKIP no name change')
        continue

    if state != state.strip():
        print("FAIL state stripped: raw='{}' stripped='{}'".format(state, state.strip()))
        continue

    if old_county != old_county.strip():
        print("FAIL county stripped: raw='{}' stripped='{}'".format(old_county, old_county.strip()))
        continue

    data = []
    for row in result:
        if row['state'] != state:
            print('FAIL: multiple states for county: county={}, state={}'.format(row['county'], ','.join(state, row['state'])))
            data = None
            break

        if row['p10k']:
            p10k = float(row['p10k'])
        else:
            p10k = None
        data.append({
            'measurement': 'infected_de_state',
            'tags': {
                'state': state,
                'county': county
            },
            'time': row['time'],
            'fields': {
                'count': row['count'],
                'p10k': p10k,
                'death': row['death']
            }
        })

    if db.write_points(data):
        db.query("""DROP SERIES FROM "infected_de_state" WHERE county = '{}'""".format(old_county))
    else:
        print('FAIL {} -> {}'.format(old_county, county))

    print('End {} -> {}'.format(old_county, county))

db.close()