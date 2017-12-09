import sys
import itertools
import pandas as pd
sys.path.insert(0, 'dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_news_tags_bq(from_date, to_date, google_key_path):
    print("news_tags")

    columns = ["Date", "constituent", "Tags", "Count", "constituent_name", "constituent_id"]

    query = """
    SELECT
  a.news_date AS Date,
  a.constituent,
  b.news_topics as Tags,
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
  Date,
  a.constituent,
  b.news_topics,
  a.constituent_name,
  a.constituent_id;
    """.format(from_date, to_date)

    storage_client = Storage(google_key_path='igenie-project-key.json')

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    try:
        storage_client.insert_bigquery_data('pecten_dataset', 'news_tag_copy', to_insert)
    except Exception as e:
        print(e)

        