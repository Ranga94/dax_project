import sys
import itertools
import pandas as pd
sys.path.insert(0, 'dax_project-master')
from utils.Storage import Storage
from datetime import datetime

def get_news_all(from_date, to_date, google_key_path):
    print("news_all")

    columns = ["NEWS_DATE_NewsDim", "score", "NEWS_PUBLICATION_NewsDim", "categorised_tag", "constituent_id", "NEWS_ARTICLE_TXT_NewsDim", "sentiment", "news_Title_NewsDim", "entity_tags", "entity_tags.FACILITY", "entity_tags.QUANTITY", "entity_tags.EVENT", "entity_tags.PERSON", "entity_tags.DATE", "entity_tags.TIME", "entity_tags.CARDINAL", "entity_tags.PRODUCT", "count"]

    query = """
    SELECT news_date as news_date_NewsDim, score, news_publication as NEWS_PUBLICATION_NewsDim, news_topics as categorised_tag, constituent_id, news_article_txt as NEWS_ARTICLE_TXT_NewsDim, sentiment, news_title as NEWS_TITLE_NewsDim, entity_tags, entity_tags.FACILITY, entity_tags.QUANTITY, entity_tags.EVENT, entity_tags.PERSON, entity_tags.DATE, entity_tags.TIME, entity_tags.PERSON, entity_tags.DATE, entity_tags.TIME, entity_tags.CARDINAL, entity_tags.PRODUCT, entity_tags.LOC, entity_tags.WORK_OF_ART, entity_tags.LAW, entity_tags.GPE, entity_tags.PERCENT, entity_tags.FAC, entity_tags.ORDINAL, entity_tags.ORG, entity_tags.NORP, entity_tags.LANGUAGE, entity_tags.MONEY, constituent_name, count(constituent_name) as count, url, news_language, news_id, news_country, news_companies, news_region, constituent
    FROM `pecten_dataset.all_news_bkp` 
    WHERE news_date between TIMESTAMP ('{}') and TIMESTAMP ('{}')
    
    """.format(from_date, to_date)

    storage_client = Storage(google_key_path='/igenie-project-key.json')

    result = storage_client.get_bigquery_data(query, iterator_flag=True)
    to_insert = []

    for item in result:
        to_insert.append(dict((k,item[k].strftime('%Y-%m-%d %H:%M:%S')) if isinstance(item[k],datetime) else
                   (k,item[k]) for k in columns))

    try:
        storage_client.insert_bigquery_data('pecten_dataset', 'all_news', to_insert)
    except Exception as e:
        print(e)