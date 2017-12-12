import sys
import itertools
import pandas as pd
sys.path.insert(0, '/dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_twitter_sentiment_count_daily(from_date, to_date, google_key_path):
    print("twitter_sentiment_count_daily")

    columns = ["constituent_name", "sentiment_score", "line", "date", "count", "constituent_id", "constituent"]

    query = """
    SELECT constituent_name, DATE(date) as date, count(date) as count, constituent_id, constituent, 
    (
    CASE 
        WHEN sentiment_score > 0.25 THEN 'positive'
        WHEN sentiment_score < -0.25 THEN 'negative'
        ELSE 'neutral'
    END) AS line
    FROM `pecten_dataset.tweets` 
    WHERE date between TIMESTAMP ('{}') and TIMESTAMP ('{}') 
    GROUP BY constituent_name, constituent, constituent_id, date, line
    ORDER BY date
    """.format(from_date, to_date)

    storage_client = Storage(google_key_path='igenie-project-key.json')

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    try:
        storage_client.insert_bigquery_data('pecten_dataset', 'twitter_sentiment_count_daily_copy', to_insert)
    except Exception as e:
        print(e)