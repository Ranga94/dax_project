import sys
import itertools
import pandas as pd
from datetime import datetime, date

def get_twitter_sentiment_popularity(args):
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils.twitter_analytics_helpers import get_parameters
    from Database.BigQuery.backup_table import backup_table, drop_backup_table  # Feature PECTEN-9
    from Database.BigQuery.data_validation import before_insert, after_insert  # Feature PECTEN-9
    from Database.BigQuery.rollback_object import rollback_object  # Feature PECTEN-9

    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')
    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    print("twitter_sentiment_popularity")
    # Feature PECTEN-9
    backup_table_name = backup_table(args.google_key_path, common_parameters["BQ_DATASET"], 'twitter_sentiment_popularity')

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

    storage_client = Storage(args.google_key_path)

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],date) else
                   (k,item[k]) for k in columns))

    #Feature PECTEN-9
    from_date = common_parameters["FROM_DATE"].strftime("%Y-%m-%d %H:%M:%S")
    to_date = common_parameters["TO_DATE"].strftime("%Y-%m-%d %H:%M:%S")
    try:
        before_insert(args.google_key_path,common_parameters["BQ_DATASET"],'twitter_sentiment_popularity',
                      from_date,to_date,storage_client)
    except AssertionError as e:
        drop_backup_table(args.google_key_path,common_parameters["BQ_DATASET"],backup_table_name)
        e.args += ("Data already exists",)
        raise

    try:
        print("Inserting into BQ")
        # Feature PECTEN-9
        if storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"],
                                            'twitter_sentiment_popularity', to_insert):
            print("Data inserted to BQ")
        else:
            drop_backup_table(args.google_key_path, common_parameters["BQ_DATASET"], backup_table_name)
            return
    except Exception as e:
        print(e)
        rollback_object(args.google_key_path, 'table', common_parameters["BQ_DATASET"], None,
                        'twitter_sentiment_popularity', backup_table_name)
        raise

    #Feature PECTEN-9
    try:
        after_insert(args.google_key_path,common_parameters["BQ_DATASET"],'twitter_sentiment_popularity',
                     from_date,to_date,storage_client)
    except AssertionError as e:
        e.args += ("No data was inserted.",)
        raise
    finally:
        drop_backup_table(args.google_key_path,common_parameters["BQ_DATASET"],backup_table_name)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The MySQL connection string')
    parser.add_argument('environment', help='production or test')
    args = parser.parse_args()
    get_twitter_sentiment_popularity(args)