import sys
import itertools
import pandas as pd
from datetime import datetime

def get_news_tags_bq(args):
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils.twitter_analytics_helpers import get_parameters
    from Database.BigQuery.backup_table import backup_table, drop_backup_table  # Feature PECTEN-9
    from Database.BigQuery.data_validation import before_insert, after_insert  # Feature PECTEN-9
    from Database.BigQuery.rollback_object import rollback_object  # Feature PECTEN-9

    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')
    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    print("news_tags")

    # Feature PECTEN-9
    backup_table_name = backup_table(args.google_key_path, common_parameters["BQ_DATASET"], 'news_tag')

    columns = ["Date", "Constituent", "Tags", "Count", "constituent_name", "constituent_id", "From_date", "To_date"]

    query = """
    SELECT
  a.news_date AS Date,
  a.constituent as Constituent,
  b.news_topics as Tags,
  COUNT(b.news_topics) AS Count,
  a.constituent_name,
  a.constituent_id,
  TIMESTAMP('{1}') as From_date, TIMESTAMP('{2}') as To_date
FROM
  `{0}.all_news` a,
(SELECT
  x.news_id,
  news_topics
FROM
  `{0}.all_news` AS x,
  UNNEST(news_topics) AS news_topics) b
WHERE a.news_id = b.news_id AND 
a.news_date BETWEEN TIMESTAMP ('{1}')
  AND TIMESTAMP ('{2}')
GROUP BY
  Date,
  a.constituent,
  b.news_topics,
  a.constituent_name,
  a.constituent_id;
    """.format(common_parameters["BQ_DATASET"],common_parameters["FROM_DATE"].strftime("%Y-%m-%d"),
               common_parameters["TO_DATE"].strftime("%Y-%m-%d"))

    storage_client = Storage(args.google_key_path)

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    #Feature PECTEN-9
    from_date = common_parameters["FROM_DATE"].strftime("%Y-%m-%d %H:%M:%S")
    to_date = common_parameters["TO_DATE"].strftime("%Y-%m-%d %H:%M:%S")
    try:
        before_insert(args.google_key_path,common_parameters["BQ_DATASET"],'news_tag',from_date,to_date,storage_client)
    except AssertionError as e:
        drop_backup_table(args.google_key_path,common_parameters["BQ_DATASET"],backup_table_name)
        e.args += ("Data already exists",)
        raise

    try:
        print("Inserting into BQ")
        # Feature PECTEN-9
        if storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_tag', to_insert):
            print("Data inserted to BQ")
        else:
            drop_backup_table(args.google_key_path, common_parameters["BQ_DATASET"], backup_table_name)
            return
    except Exception as e:
        print(e)
        rollback_object(args.google_key_path, 'table', common_parameters["BQ_DATASET"], None,
                        'news_tag', backup_table_name)
        raise

    #Feature PECTEN-9
    try:
        after_insert(args.google_key_path,common_parameters["BQ_DATASET"],'news_tag',from_date,to_date,
                     storage_client)
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
    get_news_tags_bq(args)        
