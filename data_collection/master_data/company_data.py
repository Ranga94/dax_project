from bs4 import BeautifulSoup
import re
from pymongo import MongoClient, errors
from requests.compat import urljoin
import requests
from collections import defaultdict
from pprint import pprint
import sys
from datetime import datetime
from fake_useragent import UserAgent

def create_table_records(all_data, date_of_collection, constituent_name, constituent_id):
    dividend = []
    recent_report = []

    master_data = {}
    master_data["constituent_name"] = constituent_name
    master_data["constituent_id"] = constituent_id
    master_data["date_of_collection"] = date_of_collection
    master_data['Transparency_Level_on_First_Quotation'] = None
    master_data['Market_Segment'] = None
    master_data['Trading_Model'] = None
    master_data['Country'] = None
    master_data['Branch'] = None
    master_data['Share_Type'] = None
    master_data['Sector'] = None
    master_data['Subsector'] = None

    frankfurt_trading_parameters = {}
    frankfurt_trading_parameters["constituent_name"] = constituent_name
    frankfurt_trading_parameters["constituent_id"] = constituent_id
    frankfurt_trading_parameters["date_of_collection"] = date_of_collection
    frankfurt_trading_parameters['Specialist'] = None
    frankfurt_trading_parameters['Minimum_tradeable_Unit'] = None
    frankfurt_trading_parameters['Pre_trading'] = None
    frankfurt_trading_parameters['Post_trading'] = None

    liquidity = {}
    liquidity["constituent_name"] = constituent_name
    liquidity["constituent_id"] = constituent_id
    liquidity["date_of_collection"] = date_of_collection
    liquidity['Xetra_Liquidity_Measure_XLM'] = None
    liquidity['Designated_Sponsor_s'] = None
    liquidity['Liquidity_Category'] = None
    liquidity['Liquidity_Class'] = None

    instrument_information = {}
    instrument_information["constituent_name"] = constituent_name
    instrument_information["constituent_id"] = constituent_id
    instrument_information["date_of_collection"] = date_of_collection
    instrument_information['ISIN'] = None
    instrument_information['Number_of_Shares'] = None
    instrument_information['Market_Capitalization'] = None
    instrument_information['Reuters_Instrument_Code'] = None

    trading_parameters = {}
    trading_parameters["constituent_name"] = constituent_name
    trading_parameters["constituent_id"] = constituent_id
    trading_parameters["date_of_collection"] = date_of_collection
    trading_parameters['Exchange_Symbol'] = None
    trading_parameters['Start_Intraday_Auction'] = None
    trading_parameters['End_Post_Trading'] = None
    trading_parameters['Start_Pre_Trading'] = None
    trading_parameters['Min_Quote_Size'] = None
    trading_parameters['Max_Spread'] = None
    trading_parameters['Min_Tradable_Unit'] = None
    trading_parameters['CCP_Eligible'] = None
    trading_parameters['Instrument_Type'] = None
    trading_parameters['Instrument_Subtype'] = None
    trading_parameters['Instrument_Group'] = None
    trading_parameters['Trading_Model'] = None

    business_ratio = {}
    business_ratio["constituent_name"] = constituent_name
    business_ratio["constituent_id"] = constituent_id
    business_ratio["date_of_collection"] = date_of_collection
    business_ratio['Dividend_yield'] = None
    business_ratio['Dividend_2016'] = None
    business_ratio['Market_Capitalization'] = None
    business_ratio['Number_of_Shares'] = None
    business_ratio['Win_per_share'] = None
    business_ratio['P_E_Ratio'] = None

    technical_figures = {}
    technical_figures["constituent_name"] = constituent_name
    technical_figures["constituent_id"] = constituent_id
    technical_figures["date_of_collection"] = date_of_collection
    technical_figures['Alpha_250_days'] = None
    technical_figures['Volatility_60_days'] = None
    technical_figures['Correlation_250_days'] = None
    technical_figures['Correlation_30_days'] = None
    technical_figures['Beta_250_days'] = None
    technical_figures['Beta_30_days'] = None
    technical_figures['Alpha_30_days'] = None

    historical_key_data = {}

    for data in all_data:
        if "table" in data and data["table"] == "Dividend":
            doc = {}
            doc["constituent_name"] = constituent_name
            doc["constituent_id"] = constituent_id
            doc["date_of_collection"] = date_of_collection

            if 'Dividend Cycle' in data:
                doc['Dividend_Cycle'] = data['Dividend Cycle']
            else:
                doc['Dividend_Cycle'] = None

            if 'ISIN' in data:
                doc['ISIN'] = data['ISIN']
            else:
                doc['ISIN'] = None

            if 'Last Dividend Payment' in data:
                doc['Last_Dividend_Payment'] = datetime.strptime(data['Last Dividend Payment'], "%d.%m.%Y")
            else:
                doc['Last_Dividend_Payment'] = None

            if 'Value' in data and data['Value']:
                try:
                    doc['Value'] = float(data['Value'].replace('€', '').replace(',','').strip())
                except Exception as e:
                    print(e)
                    doc['Value'] = None
            else:
                doc['Value'] = None

            dividend.append(doc)

        if "table" in data and data['table'] == 'Recent Report':
            doc = {}
            doc["constituent_name"] = constituent_name
            doc["constituent_id"] = constituent_id
            doc["date_of_collection"] = date_of_collection

            if 'Period' in data:
                doc['Period'] = data['Period']
            else:
                doc['Period'] = None

            if 'Title' in data:
                doc['Title'] = data['Title']
            else:
                doc['Title'] = None

            recent_report.append(doc)

        if "table" in data and data['table'] == 'Master Data':
            if 'Transparency Level on First Quotation' in data:
                master_data['Transparency_Level_on_First_Quotation'] = data['Transparency Level on First Quotation']
            if 'Market Segment' in data:
                master_data['Market_Segment'] = data['Market Segment']
            if 'Trading Model' in data:
                master_data['Trading_Model'] = data['Trading Model']
            if 'Country' in data:
                master_data['Country'] = data['Country']
            if 'Branch' in data:
                master_data['Branch'] = data['Branch']
            if 'Share Type' in data:
                master_data['Share_Type'] = data['Share Type']
            if 'Sector' in data:
                master_data['Sector'] = data['Sector']
            if 'Subsector' in data:
                master_data['Subsector'] = data['Subsector']

        if "table" in data and data['table'] == 'Frankfurt Trading Parameters':
            if 'Specialist' in data:
                frankfurt_trading_parameters['Specialist'] = data['Specialist']
            if 'Minimum tradeable Unit' in data and data['Minimum tradeable Unit']:
                try:
                    frankfurt_trading_parameters['Minimum_tradeable_Unit'] = float(data['Minimum tradeable Unit'].replace(',',''))
                except Exception as e:
                    print(e)
                    frankfurt_trading_parameters['Minimum_tradeable_Unit'] = None
            if 'Pre-trading' in data:
                frankfurt_trading_parameters['Pre_trading'] = data['Pre-trading']
            if 'Post-trading' in data:
                frankfurt_trading_parameters['Post_trading'] = data['Post-trading']

        if "table" in data and data['table'] == 'Liquidity':
            if 'Xetra Liquidity Measure (XLM)' in data:
                liquidity['Xetra_Liquidity_Measure_XLM'] = data['Xetra Liquidity Measure (XLM)']
            if 'Liquidity Class' in data:
                liquidity['Liquidity_Class'] = data['Liquidity Class']
            if 'Liquidity Category' in data:
                liquidity['Liquidity_Category'] = data['Liquidity Category']
            if 'Designated Sponsor(s)' in data:
                liquidity['Designated_Sponsor_s'] = data['Designated Sponsor(s)']

        if "table" in data and 'Instrument Information' in data['table']:
            if 'ISIN' in data:
                instrument_information['ISIN'] = data['ISIN']
            if 'Reuters Instrument Code' in data:
                instrument_information['Reuters_Instrument_Code'] = data['Reuters Instrument Code']
            if 'Market Capitalization' in data:
                instrument_information['Market_Capitalization'] = data['Market Capitalization']
            if 'Number of Shares' in data and data['Number of Shares']:
                try:
                    instrument_information['Number_of_Shares'] = float(data['Number of Shares'].replace(',',''))
                except Exception as e:
                    print(e)
                    instrument_information['Number_of_Shares'] = None

        if "table" in data and data['table'] == 'Trading Parameters':
            if 'Exchange Symbol' in data:
                trading_parameters['Exchange_Symbol'] = data['Exchange Symbol']
            if 'CCP Eligible' in data:
                trading_parameters['CCP_Eligible'] = data['CCP Eligible']
            if 'Instrument Type' in data:
                trading_parameters['Instrument_Type'] = data['Instrument Type']
            if 'Instrument Subtype' in data:
                trading_parameters['Instrument_Subtype'] = data['Instrument Subtype']
            if 'Instrument Group' in data:
                trading_parameters['Instrument_Group'] = data['Instrument Group']
            if 'Trading Model' in data:
                trading_parameters['Trading_Model'] = data['Trading Model']
            if 'Min Tradable Unit' in data and data['Min Tradable Unit']:
                try:
                    trading_parameters['Min_Tradable_Unit'] = float(data['Min Tradable Unit'].replace(',',''))
                except Exception as e:
                    print(e)
                    trading_parameters['Min_Tradable_Unit'] = None
            if 'Max Spread' in data and data['Max Spread']:
                try:
                    trading_parameters['Max_Spread'] = float(data['Max Spread'].replace(',',''))
                except Exception as e:
                    print(e)
                    trading_parameters['Max_Spread'] = None
            if 'Min Quote Size' in data and data['Min Quote Size']:
                try:
                    trading_parameters['Min_Quote_Size'] = float(data['Min Quote Size'].replace(',',''))
                except Exception as e:
                    print(e)
                    trading_parameters['Min_Quote_Size'] = None
            if 'Start Pre Trading' in data:
                trading_parameters['Start_Pre_Trading'] = data['Start Pre Trading']
            if 'End Post Trading' in data:
                trading_parameters['End_Post_Trading'] = data['End Post Trading']
            if 'Start Intraday Auction' in data:
                trading_parameters['Start_Intraday_Auction'] = data['Start Intraday Auction']

        if "table" in data and data['table'] == 'Business Ratio':
            if 'Dividend yield %' in data and data['Dividend yield %']:
                try:
                    business_ratio['Dividend_yield'] = float(data['Dividend yield %'].replace(',',''))
                except Exception as e:
                    print(e)
                    business_ratio['Dividend_yield'] = None
            if 'P/E Ratio' in data and data['P/E Ratio']:
                try:
                    business_ratio['P_E_Ratio'] = float(data['P/E Ratio'].replace(',',''))
                except Exception as e:
                    print(e)
                    business_ratio['P_E_Ratio'] = None
            if 'Win per share' in data and data['Win per share']:
                try:
                    business_ratio['Win_per_share'] = float(data['Win per share'].replace(',',''))
                except Exception as e:
                    print(e)
                    business_ratio['Win_per_share'] = None
            if 'Number of Shares' in data and data['Number of Shares']:
                try:
                    business_ratio['Number_of_Shares'] = float(data['Number of Shares'].replace(',',''))
                except Exception as e:
                    print(e)
                    business_ratio['Number_of_ Shares'] = None
            if 'Market Capitalization' in data:
                business_ratio['Market_Capitalization'] = data['Market Capitalization']
            if 'Dividend (2016)' in data and data['Dividend (2016)']:
                try:
                    business_ratio['Dividend_2016'] = float(data['Dividend (2016)'].replace(',',''))
                except Exception as e:
                    print(e)
                    business_ratio['Dividend_2016'] = None

        if "table" in data and data['table'] == 'Technische Kennzahlen':
            if 'Alpha 250 days' in data and data['Alpha 250 days']:
                try:
                    technical_figures['Alpha_250_days'] = float(data['Alpha 250 days'].replace(',',''))
                except Exception as e:
                    print(e)
                    technical_figures['Alpha_250_days'] = None
            if 'Alpha 30 days' in data and data['Alpha 30 days']:
                try:
                    technical_figures['Alpha_30_days'] = float(data['Alpha 30 days'].replace(',',''))
                except Exception as e:
                    print(e)
                    technical_figures['Alpha_30_days'] = None
            if 'Beta 30 days' in data and data['Beta 30 days']:
                try:
                    technical_figures['Beta_30_days'] = float(data['Beta 30 days'].replace(',',''))
                except Exception as e:
                    print(e)
                    technical_figures['Beta_30_days'] = None
            if 'Beta 250 days' in data and data['Beta 250 days']:
                try:
                    technical_figures['Beta_250_days'] = float(data['Beta 250 days'].replace(',',''))
                except Exception as e:
                    print(e)
                    technical_figures['Beta_250_days'] = None
            if 'Correlation 30 days' in data and data['Correlation 30 days']:
                try:
                    technical_figures['Correlation_30_days'] = float(data['Correlation 30 days'].replace(',',''))
                except Exception as e:
                    print(e)
                    technical_figures['Correlation_30_days'] = None
            if 'Correlation 250 days' in data and data['Correlation 250 days']:
                try:
                    technical_figures['Correlation_250_days'] = float(data['Correlation 250 days'].replace(',',''))
                except Exception as e:
                    print(e)
                    technical_figures['Correlation_250_days'] = None
            if 'Volatility 60 days' in data and data['Volatility 60 days']:
                try:
                    technical_figures['Volatility_60_days'] = float(data['Volatility 60 days'].replace(',',''))
                except Exception as e:
                    print(e)
                    technical_figures['Volatility_60_days'] = None

        if "table" in data and data['table'] == 'Historical Key Data':
            if data['year'] in historical_key_data:
                doc = historical_key_data[data['year']]
            else:
                doc = {}
                doc["constituent_name"] = constituent_name
                doc["constituent_id"] = constituent_id
                doc["date_of_collection"] = date_of_collection
                doc['year'] = data['year']
                historical_key_data[data['year']] = doc
                doc['Financial_reporting_currency'] = None
                doc['Sales_in_Mio'] = None
                doc['Dividend'] = None
                doc['Dividend_yield'] = None
                doc['Earning_per_share'] = None
                doc['PER'] = None
                doc['EBIT_in_Mio'] = None
                doc['EBITDA_in_Mio'] = None
                doc['Net_profit'] = None
                doc['net_profit_clean_in_Mio'] = None
                doc['PTP'] = None
                doc['PTP_reported'] = None
                doc['EPS_clean'] = None
                doc['EPS_reported'] = None
                doc['Gross_income'] = None
                doc['Cashflow_Investing_in_Mio'] = None
                doc['Cashflow_Operations_in_Mio'] = None
                doc['Cashflow_Financing_in_Mio'] = None
                doc['Cashflow_share'] = None
                doc['Free_Cashflow_in_Mio'] = None
                doc['Free_Cashflow_Aktie'] = None
                doc['Book_value_per_share'] = None
                doc['Net_debt_in_Mio'] = None
                doc['Research_expenses_in_Mio'] = None
                doc['Capital_expenses'] = None
                doc['Sales_expenses'] = None
                doc['Equity_capital_in_Mio'] = None
                doc['Total_assetts_in_Mio'] = None

            if 'Financial reporting currency' in data:
                doc['Financial_reporting_currency'] = data['Financial reporting currency']
            if 'Sales in Mio' in data and data['Sales in Mio']:
                try:
                    doc['Sales_in_Mio'] = float(data['Sales in Mio'].replace(',', ''))
                except Exception as e:
                    print(e)
                    doc['Sales_in_Mio'] = None
            if 'Dividend ' in data and data['Dividend ']:
                try:
                    doc['Dividend'] = float(data['Dividend '].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Dividend'] = None
            if 'Dividend yield %' in data and data['Dividend yield %']:
                try:
                    doc['Dividend_yield'] = float(data['Dividend yield %'].replace('%','').replace(',','').strip())
                except Exception as e:
                    print(e)
                    doc['Dividend_yield'] = None
            if 'Earning per share in data' in data and data['Earning per share']:
                try:
                    doc['Earning_per_share'] = float(data['Earning per share'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Earning_per_share'] = None
            if 'PER' in data and data['PER']:
                try:
                    doc['PER'] = float(data['PER'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['PER'] = None
            if 'EBIT in Mio' in data and data['EBIT in Mio']:
                try:
                    doc['EBIT_in_Mio'] = float(data['EBIT in Mio'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['EBIT_in_Mio'] = None
            if 'EBITDA in Mio' in data and data['EBITDA in Mio']:
                try:
                    doc['EBITDA_in_Mio'] = float(data['EBITDA in Mio'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['EBITDA_in_Mio'] = None
            if 'Net profit' in data and data['Net profit']:
                try:
                    doc['Net_profit'] = float(data['Net profit'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Net_profit'] = None
            if 'net profit clean in Mio' in data and data['net profit clean in Mio']:
                try:
                    doc['net_profit_clean_in_Mio'] = float(data['net profit clean in Mio'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['net_profit_clean_in_Mio'] = None
            if 'PTP' in data and data['PTP']:
                try:
                    doc['PTP'] = float(data['PTP'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['PTP'] = None
            if 'PTP reported' in data and data['PTP reported']:
                try:
                    doc['PTP_reported'] = float(data['PTP reported'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['PTP_reported'] = None
            if 'EPS clean' in data and data['EPS clean']:
                try:
                    doc['EPS_clean'] = float(data['EPS clean'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['EPS_clean'] = None
            if 'EPS reported' in data and data['EPS reported']:
                try:
                    doc['EPS_reported'] = float(data['EPS reported'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['EPS_reported'] = None
            if 'Gross income' in data and data['Gross income']:
                try:
                    doc['Gross_income'] = float(data['Gross income'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Gross_income'] = None
            if 'Cashflow (Investing) in Mio' in data and data['Cashflow (Investing) in Mio']:
                try:
                    doc['Cashflow_Investing_in_Mio'] = float(data['Cashflow (Investing) in Mio'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Cashflow_Investing_in_Mio'] = None
            if 'Cashflow (Operations) in Mio' in data and data['Cashflow (Operations) in Mio']:
                try:
                    doc['Cashflow_Operations_in_Mio'] = float(data['Cashflow (Operations) in Mio'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Cashflow_Operations_in_Mio'] = None
            if 'Cashflow (Financing) in Mio' in data and data['Cashflow (Financing) in Mio']:
                try:
                    doc['Cashflow_Financing_in_Mio'] = float(data['Cashflow (Financing) in Mio'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Cashflow_Financing_in_Mio'] = None
            if 'Cashflow/share in data' in data and data['Cashflow/share']:
                try:
                    doc['Cashflow_share'] = float(data['Cashflow/share'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Cashflow_share'] = None
            if 'Free Cashflow in Mio' in data and data['Free Cashflow in Mio']:
                try:
                    doc['Free_Cashflow_in_Mio'] = float(data['Free Cashflow in Mio'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Free_Cashflow_in_Mio'] = None
            if 'Free Cashflow/Aktie' in data and data['Free Cashflow/Aktie']:
                try:
                    doc['Free_Cashflow_Aktie'] = float(data['Free Cashflow/Aktie'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Free_Cashflow_Aktie'] = None
            if 'Book value per share' in data and data['Book value per share']:
                try:
                    doc['Book_value_per_share'] = float(data['Book value per share'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Book_value_per_share'] = None
            if 'Net debt in Mio' in data and data['Net debt in Mio']:
                try:
                    doc['Net_debt_in_Mio'] = float(data['Net debt in Mio'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Net_debt_in_Mio'] = None
            if 'Research expenses in Mio' in data and data['Research expenses in Mio']:
                try:
                    doc['Research_expenses_in_Mio'] = float(data['Research expenses in Mio'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Research_expenses_in_Mio'] = None
            if 'Capital expenses' in data and data['Capital expenses']:
                try:
                    doc['Capital_expenses'] = float(data['Capital expenses'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Capital_expenses'] = None
            if 'Sales expenses' in data and data['Sales expenses']:
                try:
                    doc['Sales_expenses'] = float(data['Sales expenses'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Sales_expenses'] = None
            if 'Equity capital in Mio' in data and data['Equity capital in Mio']:
                try:
                    doc['Equity_capital_in_Mio'] = float(data['Equity capital in Mio'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Equity_capital_in_Mio'] = None
            if 'Total assetts in Mio' in data and data['Total assetts in Mio']:
                try:
                    doc['Total_assetts_in_Mio'] = float(data['Total assetts in Mio'].replace(',',''))
                except Exception as e:
                    print(e)
                    doc['Total_assetts_in_Mio'] = None


    historical_key_data_list = [value for key, value in historical_key_data.items()]

    return dividend, recent_report, [master_data], [frankfurt_trading_parameters], [liquidity], [instrument_information], \
           [trading_parameters], [business_ratio], [technical_figures], historical_key_data_list

def main(args):
    try:
        extract_meta_data(args)
    except Exception as e:
        print(e)

def extract_meta_data(args):
    # Define random user agent object
    ua = UserAgent()
    base_url = 'http://en.boerse-frankfurt.de/stock/'
    date_of_collection = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    # Get constituents
    storage = Storage.Storage(args.google_key_path)

    columns = ["CONSTITUENT_NAME", "CONSTITUENT_ID", "URL_KEY", "COMPANY_NAME"]
    table = "PARAM_COMPANY_DATA_KEYS"

    all_constituents = storage.get_sql_data(sql_connection_string=args.param_connection_string,
                                            sql_table_name=table,
                                            sql_column_list=columns)

    for constituent_name, constituent_id, url_key, company_name in all_constituents:
        webpages = defaultdict(list)
        webpages['http://en.boerse-frankfurt.de/stock/{}-share/FSE#Master'] = ['Master Data', 'Frankfurt Trading Parameters', 'Liquidity', 'Instrument Information {}-Share'.format(company_name), 'Trading Parameters']
        webpages['http://en.boerse-frankfurt.de/stock/keydata/{}-share/FSE#Key'] = ['Business Ratio', 'Technische Kennzahlen', 'Historical Key Data']
        webpages['http://en.boerse-frankfurt.de/stock/companydata/{}-share/FSE#Company'] = ['Dividend', 'Company Events', 'Recent Report']

        insert_data = []

        company_name = None

        for page in webpages.keys():
            url = page.format(url_key)
            response = requests.get(url, headers={'User-Agent': ua.random})
            soup = BeautifulSoup(response.text, 'lxml')

            if not company_name:
                company_name = soup.find(class_='stock-headline')
                print(company_name.string)

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
                                {'table': section, 'constituent': constituent_name, 'year': year, header.string.replace('.', ''): items[year - 2012].string})

                elif section == 'Dividend':
                    master_data = soup.find('h2', string=section)
                    if master_data is None:
                        continue
                    master_data_table = master_data.parent.parent.parent
                    master_data_rows = master_data_table.find_all('tr')

                    for row in master_data_rows:
                        data = row.find_all('td')
                        if data:
                            insert_data.append({'table': section, 'constituent': constituent_name,
                                                'Last Dividend Payment': data[0].string.strip(),
                                                'Dividend Cycle': data[1].string.strip(),
                                                'Value': data[2].string.strip(),
                                                'ISIN': data[3].string.strip()})

                elif section == 'Company Events':
                    master_data = soup.find('a', href='/aktien/unternehmenskalender/{}'.format(url_key))
                    if master_data is None:
                        continue
                    master_data_table = master_data.parent.parent.parent.parent
                    master_data_rows = master_data_table.find_all('tr')

                    for row in master_data_rows:
                        data = row.find_all('td')
                        if data:
                            insert_data.append({'table': section, 'constituent': constituent_name,
                                                'Date/Time': data[0].string.strip(),
                                               'Title': data[1].string.strip(),
                                                'Location': data[2].string.strip()})


                elif section == 'Recent Report':
                    master_data = soup.find('a', href="/aktien/unternehmensberichte/{}".format(url_key))
                    if master_data is None:
                        continue
                    master_data_table = master_data.parent.parent.parent.parent
                    master_data_rows = master_data_table.find_all('tr')

                    for row in master_data_rows:
                        data = row.find_all('td')
                        if data:
                            insert_data.append({'table': section, 'constituent': constituent_name,
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

                            insert_data.append({'table': section, 'constituent': constituent_name,
                                                data[0].string.strip().replace('.', ''): data[1].string.strip()})

        dividend, recent_report, master_data, frankfurt_trading_parameters, liquidity, instrument_information, \
        trading_parameters, business_ratio, technical_figures, historical_key_data_list = create_table_records(insert_data,date_of_collection,constituent_name, constituent_id)

        try:
            print("Inserting {} into BQ".format('dividend'))
            storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'dividend', dividend)
        except Exception as e:
            print(e)
        try:
            print("Inserting {} into BQ".format('recent_report'))
            storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'recent_report', recent_report)
        except Exception as e:
            print(e)
        try:
            print("Inserting {} into BQ".format('master_data'))
            storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'master_data', master_data)
        except Exception as e:
            print(e)
        try:
            print("Inserting {} into BQ".format('frankfurt_trading_parameters'))
            storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'frankfurt_trading_parameters', frankfurt_trading_parameters)
        except Exception as e:
            print(e)
        try:
            print("Inserting {} into BQ".format('liquidity'))
            storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'liquidity', liquidity)
        except Exception as e:
            print(e)
        try:
            print("Inserting {} into BQ".format('instrument_information'))
            storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'instrument_information', instrument_information)
        except Exception as e:
            print(e)
        try:
            print("Inserting {} into BQ".format('trading_parameters'))
            storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'trading_parameters', trading_parameters)
        except Exception as e:
            print(e)
        try:
            print("Inserting {} into BQ".format('business_ratio'))
            storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'business_ratio', business_ratio)
        except Exception as e:
            print(e)
        try:
            print("Inserting {} into BQ".format('technical_figures'))
            storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'technical_figures', technical_figures)
        except Exception as e:
            print(e)
        try:
            print("Inserting {} into BQ".format('historical_key_data'))
            storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'historical_key_data', historical_key_data_list)
        except Exception as e:
            print(e)

        time.sleep(5)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The MySQL connection string')
    parser.add_argument('environment', help='production or test')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils.twitter_analytics_helpers import *
    main(args)