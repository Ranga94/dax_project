import sys
import itertools
import pandas as pd
sys.path.insert(0, '')
from utils.Storage import Storage
from datetime import datetime, date

def get_twitter_sentiment_popularity(args):
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')
    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    print("twitter_sentiment_popularity")

    columns = ["count", "constituent", "avg_sentiment_all", "constituent_name", "constituent_id", "date", "from_date", "to_date"]

    query = """
    SELECT constituent_name, constituent_id, constituent, count(text) as count, avg(sentiment_score) as avg_sentiment_all, TIMESTAMP('{1}') as from_date, TIMESTAMP('{2}') as to_date,
CASE
        WHEN date > '2017-11-17 00:00:00' THEN '2017-12-13 00:00:00 UTC'
    END AS date
FROM `{0}.tweets` 
WHERE date between TIMESTAMP('{1}') and ('{2}')
group by date, constituent_name, constituent_id, constituent order by count DESC LIMIT 10
    """.format(common_parameters["BQ_DATASET"],common_parameters["FROM_DATE"].strftime("%Y-%m-%d %H:%M:%S"),
               common_parameters["TO_DATE"].strftime("%Y-%m-%d %H:%M:%S"))

    storage_client = Storage.Storage(args.google_key_path)

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],date) else
                   (k,item[k]) for k in columns))

    try:
        print("Inserting into BQ")
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"],
                                            'twitter_sentiment_popularity', to_insert)
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
    get_twitter_sentiment_popularity(args)