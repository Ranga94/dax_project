import sys
import itertools
import pandas as pd
sys.path.insert(0, '/dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_country_data(from_date, to_date, google_key_path):
    print("country_data")

    columns = ["count", "status", "avg_sentiment", "constituent_name", "country_name", "constituent", "date_of_analysis", "constituent_id"]

    query = """
    SELECT constituent, AVG(sentiment_score) as avg_sentiment, count(place.country_code) as count, place.country_code as country_name, constituent_name, constituent_id,
    CASE
        WHEN date > '2017-12-01 00:00:00' THEN 'active'
        ELSE 'inactive'
    END AS status,
    CASE
        WHEN date > '2017-12-01 00:00:00' THEN '2017-12-01 00:00:00 UTC'
    END AS date_of_analysis
    FROM `pecten_dataset.tweets`
    WHERE date BETWEEN TIMESTAMP ('{}') and TIMESTAMP ('{}') and place.country_code is not null
    GROUP BY constituent_id, constituent, country_name, constituent_name, date     
    """.format(from_date, to_date)

    storage_client = Storage(google_key_path='igenie-project-key.json')

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    try:
        storage_client.insert_bigquery_data('pecten_dataset', 'country_data_copy', to_insert)
    except Exception as e:
        print(e)