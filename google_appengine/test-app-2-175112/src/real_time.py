import sys
from pymongo import MongoClient, errors
import requests
from datetime import datetime
import time

all_constituents = {'DAX': 'HISTORY+998032;830;814+TICKS+1', 'Allianz':'HISTORY+322646;13;814+TICKS+1', 'adidas':'HISTORY+11730015;44;814+TICKS+1',
                    'BASF':'HISTORY+11450563;44;814+TICKS+1', 'Bayer': 'HISTORY+10367293;44;814+TICKS+1', 'Beiersdorf':'HISTORY+324660;44;814+TICKS+1',
                    'BMW':'HISTORY+324410;44;814+TICKS+1', 'Commerzbank':'HISTORY+21170377;44;814+TICKS+1','Continental': 'HISTORY+327800;44;814+TICKS+1',
                    'Daimler':'HISTORY+945657;44;814+TICKS+1', 'Deutsche Bank':'HISTORY+829257;13;814+TICKS+1', 'Deutsche Borse':'HISTORY+1177233;44;814+TICKS+1',
                    'Deutsche Post': 'HISTORY+1124244;44;814+TICKS+1', 'Deutsche Telekom':'HISTORY+1124244;44;814+TICKS+1', 'EON':'HISTORY+4334819;44;814+TICKS+1',
                    'Fresenius Medical Care':'HISTORY+520878;44;814+TICKS+1', 'Fresenius':'HISTORY+332902;44;814+TICKS+1', 'HeidelbergCement':'HISTORY+335740;44;814+TICKS+1',
                    'Henkel vz':'HISTORY+335910;13;814+TICKS+1', 'Infineon':'HISTORY+1038049;44;814+TICKS+1','Linde':'HISTORY+340045;44;814+TICKS+1',
                    'Lufthansa':'HISTORY+667979;44;814+TICKS+1', 'Merck':'HISTORY+412799;44;814+TICKS+1',
                    'Munchener Ruckversicherungs-Gesellschaft':'HISTORY+341960;44;814+TICKS+1', 'ProSiebenSat1 Media':'HISTORY+21967295;13;814+TICKS+1',
                    'RWE':'HISTORY+1158883;13;814+TICKS+1', 'SAP':'HISTORY+345952;44;814+TICKS+1', 'Siemens': 'HISTORY+827766;44;814+TICKS+1',
                    'thyssenkrupp':'HISTORY+412006;44;814+TICKS+1', 'Volkswagen (VW) vz':'HISTORY+352781;44;814+TICKS+1',
                    'Vonovia':'HISTORY+21644750;13;814+TICKS+1'}

def real_time_wrapper(argv):
    base_url = "http://charts.finanzen.net/ChartData.ashx?request="

    db = get_database(argv['connection_string'])
    url = base_url + all_constituents(argv['constituent'])
    return extract_real_time_values(url, db, argv['constituent'])

def extract_real_time_values(url, database, constituent):
    response = requests.get(url)
    if response.status_code == requests.codes.ok:
        data = response.text.splitlines()[1].split(';')
        #print(data[0])
        return insert_document(database.dax_real_time, {'constituent': constituent,
                                                 'date': data[0][0:13],
                                                 'time':data[0][14:22],
                                                 'datetime': datetime.strptime(data[0], "%Y-%m-%d-%H-%M-%S-%f"),
                                                 'price': float(data[1].replace(',', ''))})
    else:
        time.sleep(60)
        response = requests.get(url)
        if response.status_code == requests.codes.ok:
            data = response.text.splitlines()[1].split(';')
            #print(data[0])
            return insert_document(database.dax_real_time, {'constituent': constituent,
                                                     'date': data[0][0:13],
                                                     'time': data[0][14:22],
                                                     'datetime': datetime.strptime(data[0], "%Y-%m-%d-%H-%M-%S-%f"),
                                                     'price': float(data[1].replace(',', ''))})

def insert_document(collection, document):
    try:
        result = collection.insert_one(document)
        #print(result)
        return result
    except errors.DuplicateKeyError as e:
        #print(e.details)
        return e.details

def get_database(connection_string):
    client = MongoClient(connection_string)
    return client.dax