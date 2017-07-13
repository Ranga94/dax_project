#!/usr/bin/env python

import sys
from pymongo import MongoClient, errors
import requests
from datetime import datetime
import time

def main(argv):
    url = "http://charts.finanzen.net/ChartData.ashx?request=HISTORY+998032;830;814+TICKS+1"
    db = get_database(argv[0])
    extract_real_time_values(url, db)

def extract_real_time_values(url, database):
    response = requests.get(url)
    if response.status_code == requests.codes.ok:
        data = response.text.splitlines()[1].split(';')

        insert_document(database.dax_real_time, {'datetime': datetime.strptime(data[0], "%Y-%m-%d-%H-%M-%S-%f"),
                                   'price': float(data[1].replace(',', ''))})
    else:
        time.sleep(60)
        response = requests.get(url)
        if response.status_code == requests.codes.ok:
            data = response.text.split(' ')[4].split(' ')

            insert_document(database.dax_real_time, {'datetime': datetime.strptime(data[0], "%Y-%m-%d-%H-%M-%S-%f"),
                                                     'price': float(data[1].string.replace(',', ''))})

def insert_document(collection, document):
    try:
        result = collection.insert_one(document)
        print(result)
    except errors.DuplicateKeyError as e:
        print(e.details)

def get_database(connection_string):
    client = MongoClient(connection_string)
    return client.dax

if __name__ == "__main__":
    main(sys.argv[1:])