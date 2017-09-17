#!/usr/bin/env python

from selenium import webdriver
from bs4 import BeautifulSoup
import re
from pymongo import MongoClient, errors
from datetime import datetime
from requests.compat import urljoin
import time

all_constituents = {'Allianz':'Allianz', 'adidas':'adidas', 'BASF':'BASF', 'Bayer':'Bayer', 'Beiersdorf':'Beiersdorf',
                    'BMW':'BMW', 'Commerzbank':'Commerzbank', 'Continental':'Continental', 'Daimler':'Daimler',
                    'Deutsche Bank':'Deutsche_Bank', 'Deutsche Börse':'Deutsche_Boerse', 'Deutsche Post':'Deutsche_Post',
                    'Deutsche Telekom':'Deutsche_Telekom', 'EON':'EON', 'Fresenius Medical Care':'Fresenius_Medical_Care',
                    'Fresenius':'Fresenius', 'HeidelbergCement':'HeidelbergCement', 'Henkel vz':'Henkel_vz', 'Infineon':'Infineon',
                    'Linde':'Linde','Lufthansa':'Lufthansa', 'Merck':'Merck', 'Münchener Rückversicherungs-Gesellschaft': 'Munich_Re',
                    'ProSiebenSat1 Media':'ProSiebenSat1_Media', 'RWE':'RWE', 'SAP':'SAP', 'Siemens':'Siemens', 'thyssenkrupp':'thyssenkrupp',
                    'Volkswagen (VW) vz':'Volkswagen_vz','Vonovia':'Vonovia'}


def main(args):
    dax_url = 'http://en.boerse-frankfurt.de/index/pricehistory/DAX/1.1.2003_4.9.2017#History'
    constituent_base_url = 'http://en.boerse-frankfurt.de/stock/pricehistory/'
    driver = webdriver.PhantomJS()
    db = get_database(args.connection_string)

    if args.all:
        extract_historical_data(dax_url, driver, db, constituent='DAX')
        for c in all_constituents.keys():
            extract_historical_data(urljoin(constituent_base_url, all_constituents[c] + '-share/FSE/1.1.2003_4.9.2017#Price_History'), driver, db, constituent=c)
            time.sleep(60)
    else:
        if args.constituent == 'DAX':
            extract_historical_data(dax_url, driver, db, constituent='DAX')
        else:
            if args.constituent in all_constituents.keys():
                constituent_url = urljoin(constituent_base_url, all_constituents[args.constituent] + '-share/FSE/1.1.2003_4.9.2017#Price_History') #check const url name
                extract_historical_data(constituent_url, driver, db, constituent=args.constituent)

    driver.quit()


def extract_historical_data(url, driver, database, constituent=None):
    driver.get(url)
    rows = []
    time.sleep(20)
    soup = BeautifulSoup(driver.page_source, 'lxml')
    table = soup.find(id=re.compile(r'grid-table-.*'))
    if table is None:
        print("Not able to load the data for " + constituent)
        return
    table_body = table.find('tbody')
    table_row = table_body.find('tr')

    if constituent == 'DAX':

        while table_row.find_next_sibling('tr'):
            data = table_row.find_all('td')
            try:
                rows.append({'constituent': 'DAX',
                             'date':datetime.strptime(data[0].string, "%d/%m/%Y"),
                             'opening_price': float(data[3].string.replace(',', '')),
                             'closing_price': float(data[2].string.replace(',', '')),
                             'daily_high': float(data[4].string.replace(',', '')),
                             'daily_low': float(data[5].string.replace(',', '')),
                             'turnover': '',
                             'volume': float(data[1].string.replace(',',''))
                })
            except AttributeError as e:
                pass

            table_row = table_row.find_next_sibling('tr')

        data = table_row.find_all('td')

        rows.append({'constituent': 'DAX',
                     'date': datetime.strptime(data[0].string, "%d/%m/%Y"),
                     'opening_price': float(data[3].string.replace(',', '')),
                     'closing_price': float(data[2].string.replace(',', '')),
                     'daily_high': float(data[4].string.replace(',', '')),
                     'daily_low': float(data[5].string.replace(',', '')),
                     'turnover': '',
                     'volume': float(data[1].string.replace(',', ''))
                     })
    else:

        while table_row.find_next_sibling('tr'):
            data = table_row.find_all('td')
            try:
                rows.append({'constituent': constituent,
                             'date':datetime.strptime(data[0].string, "%d/%m/%Y"),
                             'opening_price': float(data[1].string.replace(',', '')),
                             'closing_price': float(data[2].string.replace(',', '')),
                             'daily_high': float(data[3].string.replace(',', '')),
                             'daily_low': float(data[4].string.replace(',', '')),
                             'turnover': float(data[5].string.replace(',', '')),
                             'volume': float(data[6].string.replace(',',''))
                })
            except AttributeError as e:
                print(e)

            table_row = table_row.find_next_sibling('tr')

        data = table_row.find_all('td')

        try:
            rows.append({'constituent': constituent,
                         'date': datetime.strptime(data[0].string, "%d/%m/%Y"),
                         'opening_price': float(data[1].string.replace(',', '')),
                         'closing_price': float(data[2].string.replace(',', '')),
                         'daily_high': float(data[3].string.replace(',', '')),
                         'daily_low': float(data[4].string.replace(',', '')),
                         'turnover': float(data[5].string.replace(',', '')),
                         'volume': float(data[6].string.replace(',', ''))
                         })
        except AttributeError as e:
            print(e)



    bulk_insert(database.historical, rows)

def bulk_insert(collection, documents):
    try:
        result = collection.insert_many(documents)
        print(result)
    except errors.BulkWriteError as e:
        print(e.details)

def get_database(connection_string):
    client = MongoClient(connection_string)
    return client.dax_gcp

def insert_document(db, document):
    try:
        result = db.historical.insert_one(document)
        print(result)
    except errors.DuplicateKeyError as e:
        print(e.details)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('connection_string', help='The MongoDB connection string')
    group = parser.add_mutually_exclusive_group()
    group.add_argument("-a", "--all", action="store_true", help='save historical data for all constituents')
    group.add_argument("-c", "--constituent", help="save historical data for specific constituent")
    args = parser.parse_args()
    main(args)