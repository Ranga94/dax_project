import sys
import itertools
import pandas as pd
from datetime import datetime

def get_country_data(args):
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')
    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    print("country_data")

    #Feature PECTEN-9
    backup_table_name = backup_table(args.google_key_path,common_parameters["BQ_DATASET"],"country_data")

    columns = ["count", "avg_sentiment", "constituent_name", "country_name", "constituent", "constituent_id", "from_date", "to_date"]

    query = """
    SELECT constituent, AVG(sentiment_score) as avg_sentiment, count(place.country_code) as count, place.country_code as country_name, constituent_name, constituent_id, TIMESTAMP('{1}') as from_date, TIMESTAMP('{2}') as to_date
    FROM `{0}.tweets`
    WHERE date BETWEEN TIMESTAMP ('{1}') and TIMESTAMP ('{2}') and place.country_code is not null
    GROUP BY constituent_id, constituent, country_name, constituent_name     
    """.format(common_parameters["BQ_DATASET"],common_parameters["FROM_DATE"].strftime("%Y-%m-%d %H:%M:%S"),
               common_parameters["TO_DATE"].strftime("%Y-%m-%d %H:%M:%S"))

    storage_client = Storage.Storage(args.google_key_path)

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    #Feature PECTEN-9
    from_date = common_parameters["FROM_DATE"].strftime("%Y-%m-%d %H:%M:%S")
    to_date = common_parameters["TO_DATE"].strftime("%Y-%m-%d %H:%M:%S")
    try:
        before_insert(args.google_key_path,common_parameters["BQ_DATASET"],"country_data",from_date,to_date,storage_client)
    except AssertionError as e:
        drop_backup_table(args.google_key_path,common_parameters["BQ_DATASET"],backup_table_name)
        e.args += ("Data already exists",)
        raise

    try:
        print("Inserting into BQ")
        #Feature PECTEN-9
        if storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"],
                                           'country_data', to_insert):
            print("Data inserted to BQ")
        else:
            drop_backup_table(args.google_key_path, common_parameters["BQ_DATASET"], backup_table_name)
            return
    except Exception as e:
        print(e)
        drop_backup_table(args.google_key_path, common_parameters["BQ_DATASET"], backup_table_name)
        raise

    #Feature PECTEN-9
    try:
        after_insert(args.google_key_path,common_parameters["BQ_DATASET"],"country_data",from_date,to_date)
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
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils.twitter_analytics_helpers import *
    from Database.BigQuery.backup_table import backup_table,drop_backup_table #Feature PECTEN-9
    from Database.BigQuery.data_validation import before_insert, after_insert #Feature PECTEN-9
    get_country_data(args)