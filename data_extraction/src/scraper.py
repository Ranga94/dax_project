#!/usr/bin/env python

from selenium import webdriver
from bs4 import BeautifulSoup
import sys
import re
from pymongo import MongoClient, errors
from datetime import datetime
from requests.compat import urljoin

def main(argv):
    base_url = 'http://en.boerse-frankfurt.de/index/pricehistory/DAX/'
    param = '7.6.2017_7.7.2017'
    end_url = param + '#History'
    url = urljoin(base_url,end_url)
    print(url)
    #scrape('http://en.boerse-frankfurt.de/index/pricehistory/DAX/7.6.2017_7.7.2017#History')

def scrape(url):
    driver = webdriver.PhantomJS()
    db = get_database()
    historical_data = get_historical_data(url, driver, db)
    driver.quit()


def get_historical_data(url, driver, database):
    driver.get(url)
    rows = []
    soup = BeautifulSoup(driver.page_source, 'lxml')
    document = {}

    table = soup.find(id=re.compile(r'grid-table-.*'))
    table_body = table.find('tbody')
    table_row = table_body.find('tr')
    while table_row.find_next_sibling('tr'):
        data = table_row.find_all('td')

        insert_document(database, {'date':datetime.strptime(data[0].string, "%d/%m/%Y"),
                                    'volume': float(data[1].string.replace(',','')),
                                    'closing_price': float(data[2].string.replace(',','')),
                                    'opening_price': float(data[3].string.replace(',','')),
                                    'daily_high': float(data[4].string.replace(',','')),
                                    'daily_low': float(data[5].string.replace(',',''))})

        table_row = table_row.find_next_sibling('tr')

    data = table_row.find_all('td')
    insert_document(database, {'date': datetime.strptime(data[0].string, "%d/%m/%Y"),
                               'volume': float(data[1].string.replace(',', '')),
                               'closing_price': float(data[2].string.replace(',', '')),
                               'opening_price': float(data[3].string.replace(',', '')),
                               'daily_high': float(data[4].string.replace(',', '')),
                               'daily_low': float(data[5].string.replace(',', ''))})



def get_database():
    client = MongoClient('mongodb://admin:admin@ds019654.mlab.com:19654/dax')
    return client.dax

def insert_document(db, document):
    try:
        result = db.historical.insert_one(document)
        print(result)
    except errors.DuplicateKeyError as e:
        print(e.details)


if __name__ == "__main__":
    main(sys.argv[1:])