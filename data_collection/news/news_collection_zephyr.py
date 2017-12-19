import requests
import xml.etree.ElementTree as ET
import os
from datetime import datetime
from io import StringIO
import pandas as pd
import sys
import json
from pprint import pprint, pformat
import smtplib
from timeit import default_timer as timer


def get_historical_zephyr_ma_deals(args):
    query = """
    DEFINE P1 AS [Parameters.Currency=SESSION;],
    P2 AS [Parameters.RepeatingDimension=NrOfTargets],
    P3 AS [Parameters.RepeatingDimension=NrOfBidders],
    P5 AS [Parameters.RepeatingDimension=NrOfVendors];
    SELECT LINE RECORD_ID as record_id,
    LINE DEAL_OVERVIEW.DEAL_HEADLINE AS deal_headline,
    LINE DEAL_OVERVIEW.TGNAME USING P2 AS target,
    LINE DEAL_OVERVIEW.BDNAME USING P3 AS acquiror,
    LINE DEAL_OVERVIEW.VDNAME USING P5 AS vendor,
    LINE DEAL_OVERVIEW.DEALTYPE AS deal_type,
    LINE DEAL_OVERVIEW.DEAL_STATUS AS deal_status,
    LINE DEAL_STRUCTURE_AND_DATES.COMPLETION_DATE AS completion_date,
    LINE DEAL_STRUCTURE_AND_DATES.RUMOUR_DATE AS rumour_date,
    LINE DEAL_STRUCTURE_AND_DATES.ANNOUNCE_DATE AS announced_date,
    LINE DEAL_STRUCTURE_AND_DATES.EXPECTED_COMPLETION_DATE AS expected_completion_date,
    LINE DEAL_STRUCTURE_AND_DATES.ASSUMED_COMPLETION_DATE AS assumed_completion_date,
    LINE DEAL_STRUCTURE_AND_DATES.POSTPONED_DATE AS postponed_date,
    LINE DEAL_STRUCTURE_AND_DATES.WITHDRAWN_DATE AS withdrawn_date,
    LINE DEAL_OVERVIEW.ALLDLVALUE USING P1 AS deal_value
    FROM RemoteAccess.U ORDERBY 1 DESCENDING
    """

    #Get parametesr
    param_table = "PARAM_NEWS_COLLECTION"
    parameters_list = ['BVD_USERNAME', 'BVD_PASSWORD', "LOGGING"]
    where = lambda x: x["SOURCE"] == 'Zephyr'

    parameters = tah.get_parameters(args.param_connection_string, param_table, parameters_list, where)

    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: x["ENVIRONMENT"] == args.environment & x["STAUTS"] == 'active'

    common_parameters = tah.get_parameters(args.param_connection_string, common_table, common_list, common_where)

    #Get constituents
    soap = SOAPUtils()
    storage = Storage.Storage(args.google_key_path)

    columns = ["CONSTITUENT_ID", "CONSTITUENT_NAME", "STRATEGY"]
    table = "PARAM_NEWS_ZEPHYR_STRATEGIES"

    constituents = storage.get_sql_data(sql_connection_string=args.param_connection_string,
                                        sql_table_name=table,
                                        sql_column_list=columns)

    fields = ["record_id", "deal_headline", "target", "acquiror", "vendor", "deal_type", "deal_status", "completion_date", "deal_value",
              "rumour_date","announced_date","expected_completion_date","assumed_completion_date", "postponed_date", "withdrawn_date"]

    for constituent_id, constituent_name, strategy in constituents:
        if constituent_name != "DEUTSCHE BANK AG":
            continue
        print("Getting M&A deals for {}".format(constituent_name))
        try:
            token = soap.get_token(parameters['BVD_USERNAME'], parameters['BVD_PASSWORD'], 'zephyr')
            selection_token, selection_count = soap.find_with_strategy(token, strategy, "zephyr")
            get_data_result = soap.get_data(token, selection_token, selection_count, query, 'zephyr', timeout=None, number_of_records=selection_count)
        except Exception as e:
            print(str(e))
            soap.close_connection(token, 'zephyr')
            break
        finally:
            pass

        result = ET.fromstring(get_data_result)
        csv_result = result[0][0][0].text

        TESTDATA = StringIO(csv_result)
        try:
            df = pd.read_csv(TESTDATA, sep=",", parse_dates=["completion_date","rumour_date","announced_date",
                                                             "expected_completion_date","assumed_completion_date",
                                                             "postponed_date", "withdrawn_date"])
        except Exception as e:
            print(e)
            continue

        print(df.shape[0])
        # Make news_title column a string
        #df.astype({"news_title": str}, copy=False, errors='ignore')
        df = df[fields]

        # add constituent name, id and old name
        df["constituent_id"] = constituent_id
        df["constituent_name"] = constituent_name
        old_constituent_name = get_old_constituent_name(constituent_id)
        df["constituent"] = old_constituent_name

        data = json.loads(df.to_json(orient="records", date_format="iso"))
        print(len(data))

        for item in data:
            if "completion_date" in item and item["completion_date"]:
                date = item["completion_date"]
                ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                item["completion_date"] = ts

            if "rumour_date" in item and item["rumour_date"]:
                date = item["rumour_date"]
                ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                item["rumour_date"] = ts

            if "announced_date" in item and item["announced_date"]:
                date = item["announced_date"]
                ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                item["announced_date"] = ts

            if "expected_completion_dat" in item and item["expected_completion_dat"]:
                date = item["expected_completion_dat"]
                ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                item["expected_completion_date"] = ts

            if "assumed_completion_date" in item and item["assumed_completion_date"]:
                date = item["assumed_completion_date"]
                ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                item["assumed_completion_datee"] = ts

            if "postponed_date" in item and item["postponed_date"]:
                date = item["postponed_date"]
                ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                item["postponed_date"] = ts

            if "withdrawn_date" in item and item["withdrawn_date"]:
                date = item["withdrawn_date"]
                ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                item["withdrawn_date"] = ts

        if data:
            print("Inserting records to BQ")
            try:
                open("ma_deals.json", 'w').write("\n".join(json.dumps(e, cls=MongoEncoder) for e in data))
                #storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'ma_deals', data)
            except Exception as e:
                print(e)

            if parameters["LOGGING"]:
                doc = [{"date": datetime.now().date().strftime('%Y-%m-%d %H:%M:%S'),
                        "constituent_name": constituent_name,
                        "constituent_id": constituent_id,
                        "downloaded_deals": len(data),
                        "source": "Zephyr"}]
                logging(doc,common_parameters["BQ_DATASET"],"news_logs",storage)

        if token:
            soap.close_connection(token, 'zephyr')

def get_daily_zephyr_ma_deals(args):
    if __name__ != "__main__":
        from utils import logging_utils as logging_utils

    zephyr_query = """
        DEFINE P1 AS [Parameters.Currency=SESSION;],
        P2 AS [Parameters.RepeatingDimension=NrOfTargets],
        P3 AS [Parameters.RepeatingDimension=NrOfBidders],
        P5 AS [Parameters.RepeatingDimension=NrOfVendors];
        SELECT LINE RECORD_ID as record_id,
        LINE DEAL_OVERVIEW.DEAL_HEADLINE AS deal_headline,
        LINE DEAL_OVERVIEW.TGNAME USING P2 AS target,
        LINE DEAL_OVERVIEW.BDNAME USING P3 AS acquiror,
        LINE DEAL_OVERVIEW.VDNAME USING P5 AS vendor,
        LINE DEAL_OVERVIEW.DEALTYPE AS deal_type,
        LINE DEAL_OVERVIEW.DEAL_STATUS AS deal_status,
        LINE DEAL_STRUCTURE_AND_DATES.COMPLETION_DATE AS completion_date,
        LINE DEAL_STRUCTURE_AND_DATES.RUMOUR_DATE AS rumour_date,
        LINE DEAL_STRUCTURE_AND_DATES.ANNOUNCE_DATE AS announced_date,
        LINE DEAL_STRUCTURE_AND_DATES.EXPECTED_COMPLETION_DATE AS expected_completion_date,
        LINE DEAL_STRUCTURE_AND_DATES.ASSUMED_COMPLETION_DATE AS assumed_completion_date,
        LINE DEAL_STRUCTURE_AND_DATES.POSTPONED_DATE AS postponed_date,
        LINE DEAL_STRUCTURE_AND_DATES.WITHDRAWN_DATE AS withdrawn_date,
        LINE DEAL_OVERVIEW.ALLDLVALUE USING P1 AS deal_value
        FROM RemoteAccess.U ORDERBY 1 DESCENDING
        """

    # Get parameters
    param_table = "PARAM_NEWS_COLLECTION"
    parameters_list = ['BVD_USERNAME', 'BVD_PASSWORD', "LOGGING"]
    where = lambda x: x["SOURCE"] == 'Zephyr'

    parameters = tah.get_parameters(args.param_connection_string, param_table, parameters_list, where)

    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = tah.get_parameters(args.param_connection_string, common_table, common_list, common_where)

    # Get constituents
    soap = SOAPUtils()
    storage = Storage.Storage(args.google_key_path)

    columns = ["CONSTITUENT_ID", "CONSTITUENT_NAME", "STRATEGY"]
    table = "PARAM_NEWS_ZEPHYR_STRATEGIES"

    constituents = storage.get_sql_data(sql_connection_string=args.param_connection_string,
                                        sql_table_name=table,
                                        sql_column_list=columns)

    fields = ["record_id", "deal_headline", "target", "acquiror", "vendor", "deal_type", "deal_status",
              "completion_date", "deal_value",
              "rumour_date", "announced_date", "expected_completion_date", "assumed_completion_date", "postponed_date",
              "withdrawn_date"]

    for constituent_id, constituent_name, strategy in constituents:
        # get last deal
        query = """
                SELECT max(record_id) as max_id FROM `{}.ma_deals`
                WHERE constituent_id = '{}';
        """.format(common_parameters["BQ_DATASET"],constituent_id)

        try:
            result = storage.get_bigquery_data(query=query, iterator_flag=False)
            max_id = result[0]["max_id"]
            print("Getting M&A deals for {}".format(constituent_name))

            token = soap.get_token(parameters['BVD_USERNAME'], parameters['BVD_PASSWORD'], 'zephyr')
            selection_token, selection_count = soap.find_with_strategy(token, strategy, "zephyr")
            get_data_result = soap.get_data(token, selection_token, selection_count, zephyr_query, 'zephyr',
                                            timeout=None,
                                            number_of_records=100)

            result = ET.fromstring(get_data_result)
            csv_result = result[0][0][0].text

            TESTDATA = StringIO(csv_result)
            df = pd.read_csv(TESTDATA, sep=",", parse_dates=["completion_date", "rumour_date", "announced_date",
                                                         "expected_completion_date", "assumed_completion_date",
                                                         "postponed_date", "withdrawn_date"])

            df.astype({"record_id": int}, copy=False, errors='ignore')
            print("Retrieved {} items".format(df.shape[0]))

            df = df.loc[df["record_id"] > max_id]
            print("New records {} items".format(df.shape[0]))

            if df.shape[0] == 0:
                if token:
                    soap.close_connection(token, 'zephyr')
                continue

            df = df[fields]

            # add constituent name, id and old name
            df["constituent_id"] = constituent_id
            df["constituent_name"] = constituent_name
            old_constituent_name = get_old_constituent_name(constituent_id)
            df["constituent"] = old_constituent_name

            data = json.loads(df.to_json(orient="records", date_format="iso"))
            print(len(data))

            for item in data:
                if "completion_date" in item and item["completion_date"]:
                    date = item["completion_date"]
                    ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                    item["completion_date"] = ts

                if "rumour_date" in item and item["rumour_date"]:
                    date = item["rumour_date"]
                    ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                    item["rumour_date"] = ts

                if "announced_date" in item and item["announced_date"]:
                    date = item["announced_date"]
                    ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                    item["announced_date"] = ts

                if "expected_completion_dat" in item and item["expected_completion_dat"]:
                    date = item["expected_completion_dat"]
                    ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                    item["expected_completion_date"] = ts

                if "assumed_completion_date" in item and item["assumed_completion_date"]:
                    date = item["assumed_completion_date"]
                    ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                    item["assumed_completion_datee"] = ts

                if "postponed_date" in item and item["postponed_date"]:
                    date = item["postponed_date"]
                    ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                    item["postponed_date"] = ts

                if "withdrawn_date" in item and item["withdrawn_date"]:
                    date = item["withdrawn_date"]
                    ts = time.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                    item["withdrawn_date"] = ts

            if data:
                print("Inserting records to BQ")
                try:
                    #open("ma_deals.json", 'w').write("\n".join(json.dumps(e, cls=MongoEncoder) for e in data))
                    storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'ma_deals', data)
                except Exception as e:
                    print(e)
                    soap.close_connection(token, 'zephyr')

                if parameters["LOGGING"]:
                    doc = [{"date": datetime.now().date().strftime('%Y-%m-%d %H:%M:%S'),
                            "constituent_name": constituent_name,
                            "constituent_id": constituent_id,
                            "downloaded_deals": len(data),
                            "source": "Zephyr"}]
                    logging_utils.logging(doc,common_parameters["BQ_DATASET"],"news_logs",storage)

        except Exception as e:
            print(e)
            if token:
                soap.close_connection(token, 'zephyr')
            continue

        if token:
            soap.close_connection(token, 'zephyr')

def main(args):
    if __name__ != "__main__":
        sys.path.insert(0, args.python_path)
        from utils.Storage import Storage
        from utils.Storage import MongoEncoder
        from utils.SOAPUtils import SOAPUtils
        from utils import twitter_analytics_helpers as tah
        from utils import email_tools as email_tools
    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = tah.get_parameters(args.param_connection_string, common_table, common_list, common_where)

    get_daily_zephyr_ma_deals(args)

    q1 = """
        SELECT a.constituent_name, a.downloaded_news, a.date, a.source
        FROM
        (SELECT constituent_name, SUM(downloaded_news) as downloaded_news, DATE(date) as date, source
        FROM `{0}.news_logs`
        WHERE source = 'Zephyr'
        GROUP BY constituent_name, date, source
        ) a,
        (SELECT constituent_name, MAX(DATE(date)) as date
         FROM `{0}.news_logs`
         WHERE source = 'Zephyr'
         GROUP BY constituent_name
         ) b
         WHERE a.constituent_name = b.constituent_name AND a.date = b.date
         GROUP BY a.constituent_name, a.downloaded_news, a.date, a.source;
    """.format(common_parameters["BQ_DATASET"])

    q2 = """
         SELECT constituent_name,count(*) as count
         FROM `{}.ma_deals`
         GROUP BY constituent_name
        """.format(common_parameters["BQ_DATASET"])

    email_tools.send_mail(args.param_connection_string, args.google_key_path, "Zephyr",
              "PARAM_NEWS_COLLECTION", lambda x: x["SOURCE"] == "Zephyr", q1, q2)

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
    from utils.Storage import MongoEncoder
    from utils.SOAPUtils import SOAPUtils
    from utils import twitter_analytics_helpers as tah
    from utils.logging_utils import *
    from utils.email_tools import *
    main(args)