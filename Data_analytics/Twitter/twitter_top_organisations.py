import sys
import itertools
import pandas as pd
sys.path.insert(0, '/dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_twitter_top_organisations(from_date, to_date, google_key_path):
    print("twitter_top_organisations")

    columns = ["count", "constituent", "trend", "date_of_analysis", "status", "constituent_name", "constituent_id"]

    query = """
    SELECT
  a.constituent,
  b.entity_tags as trend,
  COUNT(b.entity_tags) AS count,
  a.constituent_name,
  a.constituent_id,
  CASE
        WHEN date > '2017-11-01 00:00:00' THEN 'active'
        ELSE 'inactive'
    END AS status,
  CASE
        WHEN date > '2017-12-01 00:00:00' THEN '2017-12-01 00:00:00 UTC'
    END AS date_of_analysis  
FROM
  `pecten_dataset.tweets` a,
(SELECT
  x.id_str,
  entity_tags
FROM
  `pecten_dataset.tweets` AS x,
  UNNEST(entity_tags.ORG) AS entity_tags) b
WHERE a.id_str= b.id_str AND 
a.date BETWEEN TIMESTAMP ('{}')
  AND TIMESTAMP ('{}')
GROUP BY
  a.constituent,
  a.constituent_name,
  a.constituent_id,
  date,
  b.entity_tags;
   
    """.format(from_date, to_date)

    storage_client = Storage(google_key_path='igenie-project-key.json')

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    try:
        storage_client.insert_bigquery_data('pecten_dataset', 'twitter_top_organisations_copy', to_insert)
    except Exception as e:
        print(e)