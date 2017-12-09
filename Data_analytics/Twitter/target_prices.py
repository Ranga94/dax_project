import sys
import itertools
import pandas as pd
sys.path.insert(0, '/dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_target_prices(from_date, to_date, google_key_path):
    print("twitter_sentiment_popularity")

    columns = ["entity_tags", "constituent", "sentiment_score", "from_date", "constituent_name", "constituent_id", "price", "to_date", "tweet_date", "date"]

    query = """
    SELECT 
  text, constituent, sentiment_score, constituent_name, constituent_id, date as tweet_date,
  GROUP_CONCAT(SPLIT(REGEXP_REPLACE(text, r'[^\d]+', '.'))) AS price,
  CASE
        WHEN date > '2017-11-01 00:00:00' THEN '2017-12-09 00:00:00 UTC'
    END AS date,
CASE
        WHEN date > '2017-11-01 00:00:00' THEN '2017-12-01 00:00:00 UTC'
    END AS from_date,
CASE
        WHEN date > '2017-11-01 00:00:00' THEN '2017-12-11 00:00:00 UTC'
    END AS to_date  
FROM [igenie-project:pecten_dataset.twitter_analytics_latest_price_tweets] 
WHERE date between TIMESTAMP ('{}') and TIMESTAMP ('{}')
 
    """.format(from_date, to_date)

    storage_client = Storage(google_key_path='igenie-project-key.json')

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    try:
        storage_client.insert_bigquery_data('pecten_dataset', 'target_prices_copy', to_insert)
    except Exception as e:
        print(e)  