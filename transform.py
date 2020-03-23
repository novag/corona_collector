#!/usr/bin/env python3

import csv
import json
import os

BASE_PATH = os.path.dirname(os.path.realpath(__file__))
STATES_CSV_PATH = os.path.join(BASE_PATH, 'states.csv')
COUNTIES_CSV_PATH = os.path.join(BASE_PATH, 'counties.csv')
CITIES_CSV_PATH = os.path.join(BASE_PATH, 'cities.csv')
POPULATION_JSON_PATH = os.path.join(BASE_PATH, 'population.json')

POPULATION = {
    'state': {},
    'county': {},
    'city': {}
}

with open(STATES_CSV_PATH, encoding='utf-8') as file:
    dict_reader = csv.DictReader(file)
    for row in dict_reader:
        state = row['short']
        population = int(row['population'])

        POPULATION['state'][state] = population

with open(COUNTIES_CSV_PATH, encoding='utf-8') as file:
    dict_reader = csv.DictReader(file)
    for row in dict_reader:
        state = row['state']
        county = row['county']
        population = int(row['population'])

        if state not in POPULATION['county']:
            POPULATION['county'][state] = {}

        POPULATION['county'][state][county] = population

with open(CITIES_CSV_PATH, encoding='utf-8') as file:
    dict_reader = csv.DictReader(file)
    for row in dict_reader:
        state = row['state']
        city = row['city']
        population = int(row['population'])

        if state not in POPULATION['city']:
            POPULATION['city'][state] = {}

        POPULATION['city'][state][city] = population

with open(POPULATION_JSON_PATH, 'w', encoding='utf-8') as file:
    json.dump(POPULATION, file, ensure_ascii=False)

