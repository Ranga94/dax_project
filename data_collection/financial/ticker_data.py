import sys
from datetime import datetime
import requests
from pprint import pprint
from fake_useragent import UserAgent

def extract_ticker_data(args):
    ua = UserAgent()
    base_url = "http://charts.finanzen.net/ChartData.ashx?request="

    #Get parameters
    param_table = "PARAM_TICKER_COLLECTION"
    parameters_list = ["LOGGING"]

    parameters = get_parameters(args.param_connection_string, param_table, parameters_list)

    #Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: x["ENVIRONMENT"] == args.environment & x["STAUTS"] == 'active'

    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    # Get constituents
    storage = Storage.Storage(args.google_key_path)

    columns = ["CONSTITUENT_ID", "CONSTITUENT_NAME", "URL_KEY"]
    table = "PARAM_TICKER_KEYS"

    all_constituents = storage.get_sql_data(sql_connection_string=args.param_connection_string,
                                        sql_table_name=table,
                                        sql_column_list=columns)

    i = 0
    for constituent_id, constituent_name, url_key in all_constituents:
        if i == 10 or i == 29:
            time.sleep(900)

        # get last deal
        query = """
                SELECT max(datetime) as max_date FROM `{}.ticker_data_copy`
                WHERE constituent_id = '{}';
        """.format(common_parameters["BQ_DATASET"], constituent_id)

        try:
            result = storage.get_bigquery_data(query=query, iterator_flag=False)
        except Exception as e:
            print(e)
            return

        max_date = result[0]["max_date"]

        print("Getting ticker data for {}".format(constituent_name))
        url = base_url + url_key + '6000'

        list_of_ticks = []

        try:
            response = requests.get(url, headers={'User-Agent': ua.random})
            if response.status_code == requests.codes.ok:
                data = response.text.splitlines()

                for item in data[1:-2]:
                    fields = item.split(';')

                    if max_date and max_date.date() == datetime.strptime(fields[0], "%Y-%m-%d-%H-%M-%S-%f").date():
                        print("Last date in ticker_data is same date as today")
                        break

                    document = {'constituent_name': constituent_name,
                                'constituent_id': constituent_id,
                                'date': fields[0][0:10],
                                'time': fields[0][11:19].replace('-', ':'),
                                'datetime': datetime.strptime(fields[0], "%Y-%m-%d-%H-%M-%S-%f").strftime('%Y-%m-%d %H:%M:%S'),
                                'open': float(fields[1].replace(',', '')),
                                'high': float(fields[2].replace(',', '')),
                                'low': float(fields[3].replace(',', '')),
                                'close': float(fields[4].replace(',', '')),
                                'volume': float(fields[5].replace(',', ''))}

                    list_of_ticks.append(document)

                if list_of_ticks:
                    try:
                        print("Inserting into BQ")
                        storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'ticker_data_copy', list_of_ticks)
                        # pprint(list_of_ticks)
                    except Exception as e:
                        print(e)

                    if parameters["LOGGING"]:
                        doc = [{"date": datetime.now().date().strftime('%Y-%m-%d %H:%M:%S'),
                                "constituent_name": constituent_name,
                                "constituent_id": constituent_id,
                                "downloaded_ticks": len(list_of_ticks)}]
                        logging(doc, common_parameters["BQ_DATASET"], "ticker_logs_copy", storage)

                i += 1

            else:
                print("Error making request, code {}. Retrying...".format(response.status_code))
                time.sleep(20)
                response = requests.get(url, headers={'User-Agent': ua.random})
                if response.status_code == requests.codes.ok:
                    data = response.text.splitlines()

                    for item in data[1:-2]:
                        fields = item.split(';')

                        document = {'constituent_name': constituent_name,
                                    'constituent_id': constituent_id,
                                    'date': fields[0][0:10],
                                    'time': fields[0][11:19].replace('-',':'),
                                    'datetime': datetime.strptime(fields[0], "%Y-%m-%d-%H-%M-%S-%f").strftime('%Y-%m-%d %H:%M:%S'),
                                    'open': float(fields[1].replace(',', '')),
                                    'high': float(fields[2].replace(',', '')),
                                    'low': float(fields[3].replace(',', '')),
                                    'close': float(fields[4].replace(',', '')),
                                    'volume': float(fields[5].replace(',', ''))}

                        list_of_ticks.append(document)

                    if list_of_ticks:
                        try:
                            print("Inserting into BQ")
                            storage.insert_bigquery_data(common_parameters["BQ_DATASET"], 'ticker_data_copy', list_of_ticks)
                        except Exception as e:
                            print(e)

                        if parameters["LOGGING"]:
                            doc = [{"date": datetime.now().date().strftime('%Y-%m-%d %H:%M:%S'),
                                    "constituent_name": constituent_name,
                                    "constituent_id": constituent_id,
                                    "downloaded_ticks": len(list_of_ticks)}]
                            logging(doc, common_parameters["BQ_DATASET"], "ticker_logs_copy", storage)

                    i += 1

                else:
                    return "Error making request, code {}. Skipping.".format(response.status_code)

        except Exception as ex:
            return str(ex)
        finally:
            time.sleep(5)

def main(args):
    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: x["ENVIRONMENT"] == args.environment & x["STAUTS"] == 'active'

    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    try:
        extract_ticker_data(args)
    except Exception as e:
        print(e)

    q1 = """
            SELECT a.constituent_name, a.downloaded_ticks, a.date
            FROM
            (SELECT constituent_name, SUM(downloaded_ticks) as downloaded_ticks, DATE(date) as date
            FROM `{0}.ticker_logs_copy`
            GROUP BY constituent_name, date
            ) a,
            (SELECT constituent_name, MAX(DATE(date)) as date
             FROM `{0}.ticker_logs_copy`
             GROUP BY constituent_name
             ) b
             WHERE a.constituent_name = b.constituent_name AND a.date = b.date
             GROUP BY a.constituent_name, a.downloaded_ticks, a.date;
        """.format(common_parameters['BQ_DATASET'])

    q2 = """
             SELECT constituent_name,count(*) as count
             FROM `{}.ticker_data_copy`
             GROUP BY constituent_name
    """.format(common_parameters["BQ_DATASET"])

    send_mail(args.param_connection_string, args.google_key_path, "Ticker",
              "PARAM_TICKER_COLLECTION", None, q1, q2)


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
    from utils.logging_utils import *
    from utils.email_tools import *
    main(args)






