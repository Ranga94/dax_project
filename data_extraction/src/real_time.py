#!/Users/Ulysses/anaconda/bin/python

import sys
from pymongo import MongoClient, errors
import requests
from datetime import datetime
import time

all_constituents = {'DAX': 'HISTORY+998032;830;814+TICKS+1', 'Allianz':'HISTORY+322646;13;814+TICKS+1', 'adidas':'HISTORY+11730015;44;814+TICKS+1',
                    'BASF':'HISTORY+11450563;44;814+TICKS+1'}

def main(argv):
    base_url = "http://charts.finanzen.net/ChartData.ashx?request="

    db = get_database(args.connection_string)
    url = base_url + all_constituents[args.constituent]
    extract_real_time_values(url, db, args.constituent)

def extract_real_time_values(url, database, constituent):
    response = requests.get(url)
    if response.status_code == requests.codes.ok:
        data = response.text.splitlines()[1].split(';')
        print(data[0])
        insert_document(database.dax_real_time, {'constituent': constituent,
                                                 'date': data[0][0:13],
                                                 'time':data[0][14:22],
                                                 'datetime': datetime.strptime(data[0], "%Y-%m-%d-%H-%M-%S-%f"),
                                                 'price': float(data[1].replace(',', ''))})
    else:
        time.sleep(60)
        response = requests.get(url)
        if response.status_code == requests.codes.ok:
            data = response.text.splitlines()[1].split(';')
            print(data[0])
            insert_document(database.dax_real_time, {'constituent': constituent,
                                                     'date': data[0][0:13],
                                                     'time': data[0][14:22],
                                                     'datetime': datetime.strptime(data[0], "%Y-%m-%d-%H-%M-%S-%f"),
                                                     'price': float(data[1].replace(',', ''))})

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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('connection_string', help='The MongoDB connection string')
    parser.add_argument("-c", "--constituent", help="save historical data for specific constituent")
    args = parser.parse_args()
    main(args)