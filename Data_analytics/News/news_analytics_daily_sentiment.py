import sys
import itertools
import pandas as pd
sys.path.insert(0, '/dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_news_analytics_daily_sentiment_bq(from_date, to_date, google_key_path):
    print("news_analytics_daily_sentiment")

    columns = ["constituent_id", "avg_sentiment", "constituent_name", "date", "constituent"]

    query = """
    SELECT constituent_id, AVG(score) as avg_sentiment, constituent_name, news_date as date, constituent
    FROM `pecten_dataset.all_news`
    WHERE news_date between TIMESTAMP ('{}') and TIMESTAMP ('{}')
    GROUP BY constituent_id, constituent_name, date, constituent
    """.format(from_date, to_date)

    storage_client = Storage(google_key_path='igenie-project-key.json')

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    try:
        storage_client.insert_bigquery_data('pecten_dataset', 'news_analytics_daily_sentiment_copy', to_insert)
    except Exception as e:
        print(e)