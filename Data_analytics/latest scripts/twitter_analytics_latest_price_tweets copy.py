import sys
import itertools
import pandas as pd
from datetime import datetime
from fuzzywuzzy import fuzz

def get_twitter_analytics_latest_price_tweets(args):
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils.twitter_analytics_helpers import *
    from Database.BigQuery.backup_table import backup_table, drop_backup_table  # Feature PECTEN-9
    from Database.BigQuery.data_validation import before_insert, after_insert  # Feature PECTEN-9
    from Database.BigQuery.rollback_object import rollback_object  # Feature PECTEN-9

    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')
    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    # Feature PECTEN-9
    backup_table_name = backup_table(args.google_key_path, common_parameters["BQ_DATASET"],
                                     'twitter_analytics_latest_price_tweets')

    print("twitter_latest_price_tweets")

    columns = ["tweet_date", "constituent_name", "from_date", "date", "text", "MONEY", "sentiment_score", "constituent", "constituent_id", "to_date"]

    query = """
    SELECT date as tweet_date,
    constituent_name, text,
    entity_tags.MONEY,
    sentiment_score,
    constituent,
    constituent_id,
    TIMESTAMP('{1}') as from_date,
    TIMESTAMP('{2}') as to_date,
CASE
        WHEN date > '2017-12-01 00:00:00' THEN '2017-12-13 00:00:00 UTC'
    END AS date
FROM `{0}.tweets`
WHERE text LIKE '%rating%'and text LIKE '%€%' and date between TIMESTAMP ('{1}') and TIMESTAMP ('{2}')
    """.format(common_parameters["BQ_DATASET"],common_parameters["FROM_DATE"].strftime("%Y-%m-%d %H:%M:%S"),
               common_parameters["TO_DATE"].strftime("%Y-%m-%d %H:%M:%S"))

    storage_client = Storage(args.google_key_path)

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []
    to_remove_indexes = set()

    for item in result:
        tweet = dict((k, item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k], datetime) else
                         (k, item[k]) for k in columns)
        tweet["text"] = " ".join([word for word in tweet["text"].split(" ") if "http" not in word])
        to_insert.append(tweet)

    for i in range(0,len(to_insert)):
        if i+1 < len(to_insert):
            for j in range(i+1,len(to_insert)):
                if to_insert[i]["text"] == to_insert[j]["text"]:
                    to_remove_indexes.add(j)

    to_insert_clean = [to_insert[i] for i in range(0,len(to_insert)) if i not in to_remove_indexes]

    # Feature PECTEN-9
    from_date = common_parameters["FROM_DATE"].strftime("%Y-%m-%d %H:%M:%S")
    to_date = common_parameters["TO_DATE"].strftime("%Y-%m-%d %H:%M:%S")
    try:
        before_insert(args.google_key_path, common_parameters["BQ_DATASET"],'twitter_analytics_latest_price_tweets' ,
                      from_date, to_date,
                      storage_client)
    except AssertionError as e:
        drop_backup_table(args.google_key_path, common_parameters["BQ_DATASET"], backup_table_name)
        e.args += ("Data already exists",)
        raise

    try:
        print("Inserting into BQ")
        # Feature PECTEN-9
        print(len(to_insert_clean))
        if storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"],
                                          'twitter_analytics_latest_price_tweets', to_insert_clean):
            print("Data inserted to BQ")
        else:
            drop_backup_table(args.google_key_path, common_parameters["BQ_DATASET"], backup_table_name)
            return
    except Exception as e:
        print(e)
        rollback_object(args.google_key_path, 'table', common_parameters["BQ_DATASET"], None,
                        'twitter_analytics_latest_price_tweets', backup_table_name)
        raise

    #Feature PECTEN-9
    try:
        after_insert(args.google_key_path,common_parameters["BQ_DATASET"],'twitter_analytics_latest_price_tweets',
                     from_date,to_date,storage_client)
    except AssertionError as e:
        e.args += ("No data was inserted.",)
        raise
    finally:
        drop_backup_table(args.google_key_path, common_parameters["BQ_DATASET"], backup_table_name)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The MySQL connection string')
    parser.add_argument('environment', help='production or test')
    args = parser.parse_args()
    get_twitter_analytics_latest_price_tweets(args)