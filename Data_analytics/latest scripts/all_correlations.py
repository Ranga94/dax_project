import sys
import itertools
import pandas as pd
sys.path.insert(0, 'dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_all_correlations(args):
    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')
    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    print("all_correlations")

    columns = ["Date", "Open", "High", "Low", "Close", "Volume", "Twitter_sent", "Constituent", "News_sent", "constituent_id", "From_date", "To_date"]

    query = """
   WITH h AS (SELECT constituent_name,constituent_id,constituent,opening_price,closing_price,
                  daily_high,daily_low, DATE(date) as date
           FROM `{0}.historical`
           WHERE date BETWEEN TIMESTAMP('{1}') AND TIMESTAMP ('{2}')),
     t AS (SELECT constituent_name,constituent_id,constituent,date,AVG(sentiment_score) as sentiment_score
           FROM (SELECT constituent_name,constituent_id,constituent,DATE(date) as date,sentiment_score
                 FROM `{0}.tweets` 
                 WHERE date BETWEEN TIMESTAMP('{1}') AND TIMESTAMP ('{2}'))
           GROUP BY date,constituent_name,constituent_id,constituent),
     n AS (SELECT constituent_name,constituent_id,constituent,news_date,AVG(score) as score
           FROM (SELECT constituent_name,constituent_id,constituent,DATE(news_date) as news_date,score
                FROM `{0}.all_news`
                WHERE news_date BETWEEN TIMESTAMP('{1}') AND TIMESTAMP ('{2}'))
           GROUP BY news_date,constituent_name,constituent_id,constituent)
SELECT
  t.date AS Date,
  n.constituent_name,
  n.constituent_id,
  n.constituent AS Constituent,
  h.opening_price AS Open,
  h.closing_price AS Close,
  h.daily_high AS High,
  h.daily_low AS Low,
  t.sentiment_score + 1 AS Twitter_sent,
  n.score + 1 AS News_sent,
  TIMESTAMP('{1}') AS From_date,
  TIMESTAMP('{2}') AS To_date
FROM
  h,t,n
WHERE
  t.date = h.date
  AND t.constituent_id = h.constituent_id
  AND h.date = n.news_date
  AND h.constituent_id = n.constituent_id
GROUP BY
  t.date,
  n.constituent_name,
  n.constituent_id,
  n.constituent,
  h.opening_price,
  h.closing_price,
  h.daily_high,
  h.daily_low,
  t.sentiment_score,
  n.score    
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
                                            'all_correlations', to_insert)
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
    get_all_correlations(args)
