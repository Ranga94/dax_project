import sys
import itertools
import pandas as pd
sys.path.insert(0, 'dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_news_analytics_topic_articles(from_date, to_date, google_key_path):
    print("news_analytics_topic_articles")

    columns = ["constituent_name", "constituent_id", "sentiment", "News_Date_NewsDim", "constituent", "News_source_NewsDim", "To_Date", "Score", "Categorised_tag", "News_Title_NewsDim", "Date", "From_Date", "NEWS_ARTICLE_TXT_NewsDim"]

    query = """
    SELECT constituent_name, constituent_id, sentiment, news_date as News_Date_NewsDim, Constituent, news_source as News_source_News_Dim, Score, news_topics as Categorised_tag, news_title as news_Title_NewsDim, news_date, news_article_txt as NEWS_ARTICLE_TXT_NewsDim 
    FROM `pecten_dataset.all_news_bkp`
    WHERE news_date between TIMESTAMP ('{}') and TIMESTAMP ('{}')
    """.format(from_date, to_date)

    storage_client = Storage(google_key_path='igenie-project-key.json')

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    try:
        storage_client.insert_bigquery_data('pecten_dataset', 'news_analytics_topic_articles_copy', to_insert)
    except Exception as e:
        print(e)