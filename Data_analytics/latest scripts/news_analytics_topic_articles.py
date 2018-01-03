import sys
import itertools
import pandas as pd
sys.path.insert(0, 'dax_project-master')
from utils.Storage import Storage
from datetime import datetime
from langdetect import detect

def get_news_analytics_topic_articles(args):
    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')
    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)

    print("news_analytics_topic_articles")

    columns = ["constituent_name", "constituent_id", "sentiment", "News_Date_NewsDim",
               "constituent", "News_source_NewsDim", "To_Date", "Score", "Categorised_tag",
               "News_Title_NewsDim", "Date", "From_Date", "NEWS_ARTICLE_TXT_NewsDim"]

    query = """
   SELECT * EXCEPT(row_num)
 FROM(
    SELECT a.constituent_name, a.constituent_id, a.sentiment, a.news_date as News_Date_NewsDim,
    a.constituent, a.news_source as News_source_NewsDim, a.Score, b.news_topics as Categorised_tag,
    a.news_title as News_Title_NewsDim, a.news_date as Date, a.news_article_txt as NEWS_ARTICLE_TXT_NewsDim,
    TIMESTAMP('{1}') as From_Date, TIMESTAMP('{2}') as To_Date,
    ROW_NUMBER() OVER (PARTITION BY constituent_name ORDER BY news_date DESC) row_num
    FROM `pecten_dataset_test.all_news` a, (SELECT
                            x.news_id,
                            news_topics
                            FROM
                           `{0}.all_news` AS x,
                            UNNEST(news_topics) AS news_topics) b
    WHERE a.news_id = b.news_id AND a.news_date between TIMESTAMP ('{1}') and TIMESTAMP ('{2}')
)
WHERE row_num between 0 and 20;
    """.format(common_parameters["BQ_DATASET"],common_parameters["FROM_DATE"].strftime("%Y-%m-%d"),
               common_parameters["TO_DATE"].strftime("%Y-%m-%d"))

    storage_client = Storage.Storage(args.google_key_path)

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []
    print(len(result))
    for item in result:
        article = dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns)
        print(detect(article["News_Title_NewsDim"]))
        if detect(article["News_Title_NewsDim"]) == 'en':
            to_insert.append(article)

    print(len(to_insert))
    return

    try:
        print("Inserting into BQ")
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_analytics_topic_articles',
                                            to_insert[:int(len(to_insert) * 0.1)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_analytics_topic_articles',
                                            to_insert[int(len(to_insert) * 0.1):int(len(to_insert) * 0.2)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_analytics_topic_articles',
                                            to_insert[int(len(to_insert) * 0.2):int(len(to_insert) * 0.3)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_analytics_topic_articles',
                                            to_insert[int(len(to_insert) * 0.3):int(len(to_insert) * 0.4)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_analytics_topic_articles',
                                            to_insert[int(len(to_insert) * 0.4):int(len(to_insert) * 0.5)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_analytics_topic_articles',
                                            to_insert[int(len(to_insert) * 0.5):int(len(to_insert) * 0.6)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_analytics_topic_articles',
                                            to_insert[int(len(to_insert) * 0.6):int(len(to_insert) * 0.7)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_analytics_topic_articles',
                                            to_insert[int(len(to_insert) * 0.7):int(len(to_insert) * 0.8)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_analytics_topic_articles',
                                            to_insert[int(len(to_insert) * 0.8):int(len(to_insert) * 0.9)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_analytics_topic_articles',
                                            to_insert[int(len(to_insert) * 0.9):])

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
    get_news_analytics_topic_articles(args)
