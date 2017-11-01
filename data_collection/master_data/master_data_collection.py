import sys
import getpass
import xml.etree.ElementTree as ET
from io import StringIO
import pandas as pd
import datetime

def main(args):
    storage = Storage()
    data = {}
    #Get parameters
    parameters = storage.get_sql_data(sql_connection_string=args.connection_string,
                                      sql_table_name="PARAM_METADATA_COLLECTION",
                                      sql_column_list=["BVD_USERNAME", "BVD_PWD", "BVD_DATABASE", "FRANKFURT_FIELDS"])

    frankfurt_fields = parameters[3].split(',')

    #Get constituent metadata
    all_constituents = storage.get_sql_data(sql_connection_string=args.connection_string,
                                            sql_table_name="PARAM_METADATA_CONSTITUENTS",
                                            sql_column_list=["CONSTITUENT_ID", "CONSTITUENT_NAME",
                                                             "URL_KEY", "BVDID"])

    print(all_constituents)

    username = getpass.getuser()

    to_insert = {"CONSTITUENT_ID": "", "CONSTITUENT_NAME": "", "ISIN":"",
                 "BVDID": "", "INDUSTRY": "",
               "COUNTRY": "", "SYMBOL": "", "ACTIVE_STATE": True, "LISTED_SINCE": datetime.datetime.today(),
               "MARKET_SECTOR": "",
               "MARKET_SUBSECTOR": "", "TRADING_MODEL": "", "REUTERS_INSTRUMENT_CODE": "",
               "SHARE_TYPE": "", "MINIMUM_TRADE_UNIT": 0.0, "WEBSITE": "",
               "LAST_UPDATE_BY": username}

    for constituent_id, _, url_key, bvdid in all_constituents:
        print(constituent_id)
        fr = website_data(url_key, frankfurt_fields)
        bvd = get_bvd_data(parameters[0], parameters[1], parameters[2], bvdid)
        data.update(fr)
        data.update(bvd)

        to_insert["CONSTITUENT_ID"] = data["SD_TICKER"] + data['BVDID']
        to_insert["CONSTITUENT_NAME"] = data['NAME']
        to_insert["ISIN"] = data['SD_ISIN']
        to_insert["BVDID"] = data['BVDID']
        to_insert["INDUSTRY"] = data['MAJOR_SECTOR']
        to_insert["COUNTRY"] = data['COUNTRY']
        to_insert["SYMBOL"] = data['SD_TICKER']
        if isinstance(data['IPO_DATE'], str):
            to_insert["LISTED_SINCE"] = datetime.datetime.strptime(data['IPO_DATE'], "%Y/%m/%d")
        to_insert["MARKET_SECTOR"] = data["Sector"]
        to_insert["MARKET_SUBSECTOR" ] = data["Subsector"]
        to_insert["TRADING_MODEL" ] = data['Trading Model']
        to_insert["REUTERS_INSTRUMENT_CODE"] = data['Reuters Instrument Code']
        to_insert["SHARE_TYPE"] = data['TYPE_SHARE']
        to_insert["MINIMUM_TRADE_UNIT"] = data['Minimum tradeable Unit']
        to_insert["WEBSITE"] = data["WEBSITE"]

        storage.insert_to_sql(sql_connection_string=args.connection_string,
                              sql_table_name="MASTER_CONSTITUENTS",
                              data=to_insert)

def get_bvd_data(user, pwd, database, bvdid):
    query = "SELECT LINE GENERAL_INFO.NAME AS NAME, LINE IDENT_CODE.ISIN AS SD_ISIN," \
            "LINE IDENT_CODE.BVDID AS BVDID,  LINE ACTIVITIES.MAJOR_SECTOR AS MAJOR_SECTOR, " \
            "LINE CONTACT_INFO.COUNTRY AS COUNTRY, LINE IDENT_CODE.TICKER AS SD_TICKER, " \
            "LINE STOCKDATA.IPO_DATE AS IPO_DATE, LINE ACTIVITIES.NATCLASS AS NATCLASS," \
            "LINE PEERGROUP.OPGNAME AS OPGNAME, LINE ACTIVITIES.PRODUCTS_SERVICES AS PRODUCTS_SERVICES," \
            " LINE STOCKDATA.TYPE_SHARE AS TYPE_SHARE," \
            "LINE CONTACT_INFO.WEBSITE USING [Parameters.DimensionSelName=NrWebSiteAddresses;NrWebSiteAddresses.Index=0;] " \
            "AS WEBSITE FROM RemoteAccess.A"

    soap = SOAPUtils()
    token = soap.get_token(user, pwd, database)
    selection_token, selection_count = soap.find_by_bvd_id(token, bvdid, database)

    try:
        get_data_result = soap.get_data(token, selection_token, selection_count, query, database)
    except Exception as e:
        print(str(e))
    finally:
        soap.close_connection(token, database)

    result = ET.fromstring(get_data_result)
    csv_result = result[0][0][0].text

    TESTDATA = StringIO(csv_result)
    df = pd.read_csv(TESTDATA, sep=",")

    return df.to_dict(orient="records")[0]

def website_data(constituent, frankfurt_fields):
    scraper = WebScraper()
    return scraper.get_frankfurt_data(constituent=constituent, desired_fields=frankfurt_fields)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('connection_string', help='The connection string')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.WebScraper import WebScraper
    from utils.SOAPUtils import SOAPUtils
    from utils.Storage import Storage
    args = parser.parse_args()
    main(args)

