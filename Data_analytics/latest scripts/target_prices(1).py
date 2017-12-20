import sys
import itertools
import pandas as pd
from datetime import datetime

def get_target_prices(args):
    # Get dataset name and dates
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)


    print("target_prices")

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
        ARRAY_TO_STRING(a.entity_tags.MONEY, ",") AS entity_tags,
        a.date AS tweet_date,
        format_price(a.text) AS price,
        b.closing_price,
        TIMESTAMP('{1}') AS from_date,
        TIMESTAMP('{2}') AS to_date,
        CASE
          WHEN a.date > '2017-11-17 00:00:00' THEN '2017-12-13 00:00:00 UTC'
        END AS date
      FROM
        {0}.twitter_analytics_latest_price_tweets a,
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

    storage_client = Storage.Storage(google_key_path=args.google_key_path)

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k, item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k], datetime) else
                              (k, item[k]) for k in columns))

    try:
        print("Inserting to BQ")
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'target_prices', to_insert)
        print(len(to_insert))
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
    get_target_prices(args)