import sys
import itertools
import pandas as pd
sys.path.insert(0, '/dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_news_analytics_daily_sentiment_bq(args):
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')
    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    print("news_analytics_daily_sentiment")

    columns = ["constituent_id", "avg_sentiment", "constituent_name", "date", "constituent", "To_date", "From_date"]

    query = """
    SELECT constituent_id, AVG(score) as avg_sentiment, constituent_name, news_date as date, constituent, TIMESTAMP('{1}') as From_date, TIMESTAMP('{2}') as To_date
    FROM `{0}.all_news`
    WHERE news_date between TIMESTAMP ('{1}') and TIMESTAMP ('{2}')
    GROUP BY constituent_id, constituent_name, date, constituent
    """.format(common_parameters["BQ_DATASET"],common_parameters["FROM_DATE"].strftime("%Y-%m-%d"),
               common_parameters["TO_DATE"].strftime("%Y-%m-%d"))

    storage_client = Storage.Storage(args.google_key_path)

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    try:
        print("Inserting into BQ")
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"],
                                            'news_analytics_daily_sentiment', to_insert)
    except Exception as e:
        print(e)
    
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
    get_news_analytics_daily_sentiment_bq(args)
