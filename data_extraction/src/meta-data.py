from bs4 import BeautifulSoup
import re
from pymongo import MongoClient, errors
from requests.compat import urljoin
import requests
from collections import defaultdict

all_constituents = {'Allianz':'Allianz', 'adidas':'adidas', 'BASF':'BASF', 'Bayer':'Bayer', 'Beiersdorf':'Beiersdorf',
                    'BMW':'BMW', 'Commerzbank':'Commerzbank', 'Continental':'Continental', 'Daimler':'Daimler',
                    'Deutsche Bank':'Deutsche_Bank', 'Deutsche Börse':'Deutsche_Boerse', 'Deutsche Post':'Deutsche_Post',
                    'Deutsche Telekom':'Deutsche_Telekom', 'EON':'EON', 'Fresenius Medical Care':'Fresenius_Medical_Care',
                    'Fresenius':'Fresenius', 'HeidelbergCement':'HeidelbergCement', 'Henkel vz':'Henkel_vz', 'Infineon':'Infineon',
                    'Linde':'Linde','Lufthansa':'Lufthansa', 'Merck':'Merck', 'Münchener Rückversicherungs-Gesellschaft': 'Munich_Re',
                    'ProSiebenSat1 Media':'ProSiebenSat1_Media', 'RWE':'RWE', 'SAP':'SAP', 'Siemens':'Siemens', 'thyssenkrupp':'thyssenkrupp',
                    'Volkswagen (VW) vz':'Volkswagen_vz','Vonovia':'Vonovia'}

base_url = 'http://en.boerse-frankfurt.de/stock/'


def main(args):
    db = get_database(args.connection_string)

    if args.all:
        for c in all_constituents.keys():
            extract_meta_data(db, c)
    else:
        extract_meta_data(db, args.constituent)



def extract_meta_data(database, constituent):
    webpages = defaultdict(list)
    webpages['http://en.boerse-frankfurt.de/stock/{}-share/FSE#Master'] = ['Master Data', 'Frankfurt Trading Parameters', 'Liquidity', 'Instrument Information {}-Share'.format(constituent), 'Trading Parameters']
    webpages['http://en.boerse-frankfurt.de/stock/keydata/{}-share/FSE#Key'] = ['Business Ratio', 'Technische Kennzahlen', 'Historical Key Data']
    webpages['http://en.boerse-frankfurt.de/stock/companydata/{}-share/FSE#Company'] = ['Dividend', 'Company Events', 'Recent Report']

    insert_data = []

    for page in webpages.keys():
        url = page.format(all_constituents[constituent])
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')


        for section in webpages[page]:
            if section == 'Historical Key Data':
                table = soup.find(class_='table balance-sheet')
                if table is None:
                    continue
                table_rows = table.find_all('tr')[1:]

                for row in table_rows:
                    header = row.find('th')
                    items = row.find_all('td')
                    for year in [2012, 2013, 2014, 2015, 2016]:
                        insert_data.append(
                            {'table': section, 'constituent': constituent, 'year': year, header.string.replace('.', ''): items[year - 2012].string})

            elif section == 'Dividend':
                master_data = soup.find('h2', string=section)
                if master_data is None:
                    continue
                master_data_table = master_data.parent.parent.parent
                master_data_rows = master_data_table.find_all('tr')

                for row in master_data_rows:
                    data = row.find_all('td')
                    if data:
                        insert_data.append({'table': section, 'constituent': constituent,
                                            'Last Dividend Payment': data[0].string.strip(),
                                            'Dividend Cycle': data[1].string.strip(),
                                            'Value': data[2].string.strip(),
                                            'ISIN': data[3].string.strip()})

            elif section == 'Company Events':
                master_data = soup.find('a', href='/aktien/unternehmenskalender/{}'.format(all_constituents[constituent]))
                if master_data is None:
                    continue
                master_data_table = master_data.parent.parent.parent.parent
                master_data_rows = master_data_table.find_all('tr')

                for row in master_data_rows:
                    data = row.find_all('td')
                    if data:
                        insert_data.append({'table': section, 'constituent': constituent,
                                            'Date/Time': data[0].string.strip(),
                                           'Title': data[1].string.strip(),
                                            'Location': data[2].string.strip()})


            elif section == 'Recent Report':
                master_data = soup.find('a', href="/aktien/unternehmensberichte/{}".format(all_constituents[constituent]))
                if master_data is None:
                    continue
                master_data_table = master_data.parent.parent.parent.parent
                master_data_rows = master_data_table.find_all('tr')

                for row in master_data_rows:
                    data = row.find_all('td')
                    if data:
                        insert_data.append({'table': section, 'constituent': constituent,
                                            'Period': data[0].string.strip(),
                                            'Title': data[1].string.strip()})

            else:
                master_data = soup.find('h2', string=section)
                if master_data is None:
                    continue
                master_data_table = master_data.parent.parent.parent
                master_data_rows = master_data_table.find_all('tr')

                for row in master_data_rows:
                    data = row.find_all('td')
                    if data:
                        insert_data.append({'table': section, 'constituent': constituent,
                                            data[0].string.strip().replace('.', ''): data[1].string.strip()})

    bulk_insert(database.company_data, insert_data)


def bulk_insert(collection, documents):
    try:
        result = collection.insert_many(documents)
        print(result)
    except errors.BulkWriteError as e:
        print(e.details)

def get_database(connection_string):
    client = MongoClient(connection_string)
    return client.dax

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