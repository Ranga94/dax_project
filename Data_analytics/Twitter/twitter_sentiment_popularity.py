import sys
import itertools
import pandas as pd
sys.path.insert(0, '/dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_twitter_sentiment_popularity(from_date, to_date, google_key_path):
    print("twitter_sentiment_popularity")

    columns = ["count", "constituent", "avg_sentiment_all", "constituent_name", "constituent_id", "date"]

    query = """
    SELECT constituent, avg(sentiment_score) as avg_sentiment_all, constituent_name, constituent_id, date, count(text) as count 
    FROM `igenie-project.pecten_dataset.tweets`
    WHERE date between TIMESTAMP ('{}') and TIMESTAMP ('{}') 
    GROUP BY constituent, constituent_name, constituent_id, date
    """.format(from_date, to_date)

    storage_client = Storage(google_key_path='igenie-project-key.json')

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    try:
        storage_client.insert_bigquery_data('pecten_dataset', 'twitter_sentiment_popularity_copy', to_insert)
    except Exception as e:
        print(e) 