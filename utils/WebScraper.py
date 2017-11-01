from collections import defaultdict
import requests
from bs4 import BeautifulSoup


class WebScraper:
    def __init__(self):
        pass

    def get_frankfurt_data(self, constituent=None, desired_fields=[], desired_section=None):
        webpages = defaultdict(list)
        webpages['http://en.boerse-frankfurt.de/stock/{}-share/FSE#Master'] = ['Master Data',
                                                                               'Frankfurt Trading Parameters',
                                                                               'Liquidity',
                                                                               'Instrument Information {}-Share'.format(
                                                                                   constituent), 'Trading Parameters']
        webpages['http://en.boerse-frankfurt.de/stock/keydata/{}-share/FSE#Key'] = ['Business Ratio',
                                                                                    'Technische Kennzahlen',
                                                                                    'Historical Key Data']
        webpages['http://en.boerse-frankfurt.de/stock/companydata/{}-share/FSE#Company'] = ['Dividend',
                                                                                            'Company Events',
                                                                                            'Recent Report']
        insert_data = []

        results = {}

        for page in webpages.keys():
            url = page.format(constituent)
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
                                {'table': section, 'constituent': constituent, 'year': year,
                                 header.string.replace('.', ''): items[year - 2012].string})

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
                    master_data = soup.find('a', href='/aktien/unternehmenskalender/{}'.format(constituent))
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
                    master_data = soup.find('a', href="/aktien/unternehmensberichte/{}".format(constituent))
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
                            if key in desired_fields:
                                results[key] = value

        return results