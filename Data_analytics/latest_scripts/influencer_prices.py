import sys
import itertools
import pandas as pd
from datetime import datetime

def get_target_prices(args):
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils.twitter_analytics_helpers import get_parameters
    from Database.BigQuery.backup_table import backup_table, drop_backup_table  # Feature PECTEN-9
    from Database.BigQuery.data_validation import before_insert, after_insert  # Feature PECTEN-9
    from Database.BigQuery.rollback_object import rollback_object  # Feature PECTEN-9

    # Get dataset name and dates
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    print("influencer_prices")

    # Feature PECTEN-9
    backup_table_name = backup_table(args.google_key_path, common_parameters["BQ_DATASET"], 'influencer_prices')

    columns = ["entity_tags", "constituent", "sentiment_score", "from_date", "constituent_name", "constituent_id",
               "price", "to_date", "tweet_date", "date"]

    query = r"""
CREATE TEMPORARY FUNCTION
  format_price(input_txt STRING)
  RETURNS FLOAT64
  LANGUAGE js AS '''
  result = null
  input_txt.split(" ").forEach(function(x){{
  new_value = parseFloat(x.replace('€',''));
  if (!isNaN(new_value)){{
    result = new_value;
  }}
  }});
  return result
    ''';
    SELECT
      * EXCEPT(closing_price)
    FROM (
      SELECT
        a.text,
        a.constituent,
        a.sentiment_score,
        a.constituent_name,
        a.constituent_id,
        NULL AS entity_tags,
        a.date AS tweet_date,
        format_price(a.text) AS price,
        b.closing_price,
        TIMESTAMP('{1}') AS from_date,
        TIMESTAMP('{2}') AS to_date,
        CASE
          WHEN a.date > '2017-11-17 00:00:00' THEN '2017-12-13 00:00:00 UTC'
        END AS date
      FROM
        {0}.influencer_price_tweets a,
        (
        SELECT
          * EXCEPT(row_num)
        FROM (
          SELECT
            closing_price,
            constituent_id,
            ROW_NUMBER() OVER (PARTITION BY constituent_id ORDER BY date DESC) row_num
          FROM
            `{0}.historical`
          WHERE
            date BETWEEN TIMESTAMP('{1}')
            AND TIMESTAMP('{2}') )
        WHERE
          row_num = 1 ) b
      WHERE
        a.constituent_id = b.constituent_id
        AND a.tweet_date BETWEEN TIMESTAMP('{1}')
        AND TIMESTAMP('{2}') )
    WHERE
      price BETWEEN closing_price*0.7
      AND closing_price*1.3;
""".format(common_parameters["BQ_DATASET"],common_parameters["FROM_DATE"].strftime('%Y-%m-%d %H:%M:%S'),
               common_parameters["TO_DATE"].strftime('%Y-%m-%d %H:%M:%S'))

    storage_client = Storage(google_key_path=args.google_key_path)

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k, item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k], datetime) else
                              (k, item[k]) for k in columns))

    #Feature PECTEN-9
    from_date = common_parameters["FROM_DATE"].strftime("%Y-%m-%d %H:%M:%S")
    to_date = common_parameters["TO_DATE"].strftime("%Y-%m-%d %H:%M:%S")
    try:
        before_insert(args.google_key_path,common_parameters["BQ_DATASET"],'influencer_prices',
                      from_date,to_date,storage_client)
    except AssertionError as e:
        drop_backup_table(args.google_key_path,common_parameters["BQ_DATASET"],backup_table_name)
        e.args += ("Data already exists",)
        raise

    try:
        print("Inserting to BQ")
        # Feature PECTEN-9
        if storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'influencer_prices', to_insert):
            print(len(to_insert))
            print("Data inserted to BQ")
        else:
            drop_backup_table(args.google_key_path, common_parameters["BQ_DATASET"], backup_table_name)
            return
    except Exception as e:
        print(e)
        rollback_object(args.google_key_path, 'table', common_parameters["BQ_DATASET"], None,
                        'influencer_prices', backup_table_name)
        raise

    #Feature PECTEN-9
    try:
        after_insert(args.google_key_path,common_parameters["BQ_DATASET"],'influencer_prices',from_date,to_date,
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
    get_target_prices(args)