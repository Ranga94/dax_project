import sys
import itertools
import pandas as pd
sys.path.insert(0, '')
from utils.Storage import Storage
from datetime import datetime
from fuzzywuzzy import fuzz

def get_twitter_analytics_latest_price_tweets(args):
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')
    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    print("twitter_latest_price_tweets")

    columns = ["tweet_date", "constituent_name", "from_date", "date", "text", "MONEY", "sentiment_score", "constituent", "constituent_id", "to_date"]

    query = """
    SELECT date as tweet_date, constituent_name, text, entity_tags.MONEY, sentiment_score, constituent, constituent_id, TIMESTAMP('{1}') as from_date, TIMESTAMP('{2}') as to_date,  
CASE
        WHEN date > '2017-12-01 00:00:00' THEN '2017-12-13 00:00:00 UTC'
    END AS date
FROM `{0}.tweets`
WHERE text LIKE '%rating%'and text LIKE '%€%' and date between TIMESTAMP ('{1}') and TIMESTAMP ('{2}')

    """.format(common_parameters["BQ_DATASET"],common_parameters["FROM_DATE"].strftime("%Y-%m-%d %H:%M:%S"),
               common_parameters["TO_DATE"].strftime("%Y-%m-%d %H:%M:%S"))

    storage_client = Storage.Storage(args.google_key_path)

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []
    to_remove_indexes = set()

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    for i in range(0,len(to_insert)):
        if i+1 < len(to_insert):
            for j in range(i+1,len(to_insert)):
                if fuzz.ratio(to_insert[i]["text"], to_insert[j]["text"]) >= 90:
                    to_remove_indexes.add(j)

    to_insert_clean = [to_insert[i] for i in range(0,len(to_insert)) if i not in to_remove_indexes]

    try:
        print("Inserting into BQ")
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"],
                                          'twitter_analytics_latest_price_tweets', to_insert_clean)
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
    get_twitter_analytics_latest_price_tweets(args)
    