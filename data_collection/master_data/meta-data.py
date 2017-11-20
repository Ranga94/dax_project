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
            #print(c)
            extract_meta_data(db, c)
    else:
        extract_meta_data(db, args.constituent)

def extract_meta_data(database, constituent):
    webpages = defaultdict(list)
    webpages['http://en.boerse-frankfurt.de/stock/{}-share/FSE#Master'] = ['Master Data', 'Frankfurt Trading Parameters', 'Liquidity', 'Instrument Information {}-Share'.format(constituent), 'Trading Parameters']
    webpages['http://en.boerse-frankfurt.de/stock/keydata/{}-share/FSE#Key'] = ['Business Ratio', 'Technische Kennzahlen', 'Historical Key Data']
    webpages['http://en.boerse-frankfurt.de/stock/companydata/{}-share/FSE#Company'] = ['Dividend', 'Company Events', 'Recent Report']

    insert_data = []

    data = {"CONSTITUENT_ID":None, "CONSTITUENT_NAME":None, "BVDID":None, "INDUSTRY":None,
            "COUNTRY":None, "SYMBOL":None, "ACTIVE_STATE":None, "LISTED_SINCE":None, "MARKET_SECTOR":None,
            "MARKET_SUBSECTOR":None, "TRADING_MODEL":None, "REUTERS_INSTRUMENT_CODE":None,
            "SHARE_TYPE":None, "MINIMUM_TRADE_UNIT":None, "WEBSITE":None, "LAST_UPDATE_DATE":None,
            "LAST_UPDATE_BY":None}

    fields = ["ISIN", "Country", "Exchange Symbol", "Admission Date", "Sector",
              "Subsector", "Trading Model", "Reuters Instrument Code", "Share Type",
              "Minimum tradeable Unit", "Web"]
    '''
     ISIN| varchar(225) | NO | | NULL | |
    | INDUSTRY | varchar(255) | NO | | NULL | |
    | COUNTRY | varchar(255) | NO | | NULL | |
    | SYMBOL | varchar(255) | NO | | NULL | |
    | ACTIVE_STATE | tinyint(1) | NO | | NULL | |
    | LISTED_SINCE | date | NO | | NULL | |
    | MARKET_SECTOR | varchar(255) | NO | | NULL | |
    | MARKET_SUBSECTOR | varchar(255) | YES | | NULL | |
    | TRADING_MODEL | varchar(255) | YES | | NULL | |
    | REUTERS_INSTRUMENT_CODE | varchar(255) | NO | | NULL | |
    | SHARE_TYPE | varchar(255) | NO | | NULL | |
    | MINIMUM_TRADE_UNIT | float | NO | | NULL | |
    | WEBSITE | varchar(255) | YES | | NULL | |
    | LAST_UPDATE_DATE | timestamp | NO | | CURRENT_TIMESTAMP | on
    update
    CURRENT_TIMESTAMP |
    | LAST_UPDATE_BY | varchar(255) | NO | | NULL | |
    | NAME | varchar(255) | NO | | NULL | |
    | BVDID
    '''

    company_name = None

    for page in webpages.keys():
        url = page.format(all_constituents[constituent])
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'lxml')

        if not company_name:
            company_name = soup.find(class_='stock-headline')
            print(company_name.string)

        for section in webpages[page]:
            if section == 'Historical Key Data':
                continue
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
                continue
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
                continue
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
                continue
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
                        key = data[0].string.strip().replace('.', '')
                        value = data[1].string.strip()
                        if key in fields:
                            print("{}:{}".format(key, value))



                        insert_data.append({'table': section, 'constituent': constituent,
                                            data[0].string.strip().replace('.', ''): data[1].string.strip()})


    bulk_insert(database.company_data_bkp, insert_data)


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
    group.add_argument("-a", "--all", action="store_true", help='save company data for all constituents')
    group.add_argument("-c", "--constituent", help="save company data for specific constituent")
    args = parser.parse_args()
    main(args)