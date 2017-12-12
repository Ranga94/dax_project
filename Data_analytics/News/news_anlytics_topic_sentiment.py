import sys
import itertools
import pandas as pd
sys.path.insert(0, 'dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_news_analytics_topic_sentiment_bq(from_date, to_date, google_key_path):
    print("news_analytics_topic_sentiment")

    columns = ["constituent_id", "overall_sentiment", "categorised_tag", "constituent_name", "count", "date", "constituent"]

    query = """
    SELECT
  a.news_date AS date,
  a.constituent,
  a.sentiment as overall_sentiment,
  b.news_topics as categorised_tag,
  COUNT(b.news_topics) AS Count,
  a.constituent_name,
  a.constituent_id
FROM
  `pecten_dataset.all_news_bkp` a,
(SELECT
  x.news_id,
  news_topics
FROM
  `pecten_dataset.all_news_bkp` AS x,
  UNNEST(news_topics) AS news_topics) b
WHERE a.news_id = b.news_id AND 
a.news_date BETWEEN TIMESTAMP ('{}')
  AND TIMESTAMP ('{}')
GROUP BY
  a.constituent_id,
  a.sentiment,
  b.news_topics,
  a.constituent_name,
  a.constituent_id,
  date,
  a.constituent;
    """.format(from_date, to_date)

    storage_client = Storage(google_key_path='igenie-project-key.json')

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    try:
        storage_client.insert_bigquery_data('pecten_dataset', 'news_analytics_topic_sentiment_copy', to_insert)
    except Exception as e:
        print(e)