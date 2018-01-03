import sys
import itertools
import pandas as pd
#sys.path.insert(0, 'dax_project-master')
from utils.Storage import Storage
from datetime import datetime
from langdetect import detect

def get_news_all(args):
    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET", "FROM_DATE", "TO_DATE"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')
    common_parameters = get_parameters(args.param_connection_string, common_table, common_list, common_where)
    print("news_all")

    #columns = ["NEWS_DATE_NewsDim", "score", "NEWS_PUBLICATION_NewsDim", "categorised_tag", "constituent_id", "NEWS_ARTICLE_TXT_NewsDim", "sentiment", "NEWS_TITLE_NewsDim", "entity_tags", "entity_tags.FACILITY", "entity_tags.QUANTITY", "entity_tags.EVENT", "entity_tags.PERSON", "entity_tags.DATE", "entity_tags.TIME", "entity_tags.CARDINAL", "entity_tags.PRODUCT", "count"]
    '''
    columns = ["NEWS_DATE_NewsDim", "score", "NEWS_PUBLICATION_NewsDim",
               "constituent_id", "NEWS_ARTICLE_TXT_NewsDim", "sentiment",
               "NEWS_TITLE_NewsDim", "entity_tags", "count", "To_date", "From_date","constituent_name",
               "constituent"]
    '''
    columns = ["NEWS_DATE_NewsDim", "score", "NEWS_PUBLICATION_NewsDim",
               "constituent_id", "NEWS_ARTICLE_TXT_NewsDim", "sentiment",
               "NEWS_TITLE_NewsDim", "To_date", "From_date", "constituent_name",
               "constituent"]

    query = """
    SELECT * EXCEPT(row_num)
    FROM(
        SELECT
        news_date AS NEWS_DATE_NewsDim,
        score,
        news_publication AS NEWS_PUBLICATION_NewsDim,
        constituent_id,
        news_article_txt AS NEWS_ARTICLE_TXT_NewsDim,
        sentiment,
        news_title AS NEWS_TITLE_NewsDim,
        entity_tags,
        1 as count,
        constituent_name,
        url,
        news_language,
        news_id,
        news_country,
        news_companies,
        news_region,
        constituent, TIMESTAMP('{1}') as From_date, TIMESTAMP('{2}') as To_date,
        ROW_NUMBER() OVER (PARTITION BY constituent_name ORDER BY news_date DESC) row_num
        FROM `{0}.all_news`
        WHERE news_date between TIMESTAMP('{1}') and TIMESTAMP('{2}')
    )
    WHERE row_num between 0 and 3;
    """.format(common_parameters["BQ_DATASET"],common_parameters["FROM_DATE"].strftime("%Y-%m-%d"),
               common_parameters["TO_DATE"].strftime("%Y-%m-%d"))

    storage_client = Storage.Storage(args.google_key_path)

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []
    ids = set()

    for item in result:
        if detect(item["NEWS_ARTICLE_TXT_NewsDim"]) == 'en':
            if item['constituent_id'] not in ids:
                to_insert.append(dict((k, item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k], datetime) else
                                      (k, item[k]) for k in columns))
                ids.add(item["constituent_id"])
            else:
                continue
        else:
            continue

    try:
        print("Inserting to BQ")
        print(len(to_insert))
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_all',to_insert)
        '''
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_all',
                                            to_insert[:int(len(to_insert) * 0.25)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_all',
                                            to_insert[int(len(to_insert) * 0.25):int(len(to_insert) * 0.5)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_all',
                                            to_insert[int(len(to_insert) * 0.5):int(len(to_insert) * 0.75)])
        storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], 'news_all',
                                            to_insert[int(len(to_insert) * 0.75):])
        '''
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
    get_news_all(args)