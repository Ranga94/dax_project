import requests
from pprint import pprint
import sys
from datetime import datetime
import time

def get_stocktwits(args):
    from utils import logging_utils as logging_utils
    from utils.Storage import Storage
    from utils import twitter_analytics_helpers as tah
    from utils.TaggingUtils import TaggingUtils as TU


    # get constituents
    storage = Storage(args.google_key_path)
    tagger = TU()

    columns = ["CONSTITUENT_ID", "CONSTITUENT_NAME", "URL_KEY"]
    table = "PARAM_STOCKTWITS_KEYS"

    all_constituents = storage.get_sql_data(sql_connection_string=args.param_connection_string,
                                        sql_table_name=table,
                                        sql_column_list=columns)

    # Get parameters
    param_table = "PARAM_STOCKTWITS_COLLECTION"
    parameters_list = ["LOGGING","DESTINATION_TABLE","LOGGING_TABLE"]

    parameters = tah.get_parameters(args.param_connection_string, param_table, parameters_list)

    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = tah.get_parameters(args.param_connection_string, common_table, common_list, common_where)

    for constituent_id, constituent_name, url_key in all_constituents:
        #Get the last id
        query = """
                SELECT max(id) as max_id FROM `{}.{}`
                WHERE constituent_id = '{}' and source = 'StockTwits'
        """.format(common_parameters["BQ_DATASET"],parameters["DESTINATION_TABLE"],constituent_id)

        try:
            result = storage.get_bigquery_data(query=query, iterator_flag=False)
            max_id = result[0]["max_id"]
        except Exception as e:
            max_id = None
            continue

        print("Gettins stocktwits for {}:{}".format(constituent_name, url_key))
        response = requests.get("https://api.stocktwits.com/api/2/streams/symbol/{}.json".format(url_key))
        to_insert = []

        if response.status_code == 200:
            for item in response.json()["messages"]:
                if max_id:
                    if int(item['id']) < max_id:
                        continue
                doc = tah.create_tweet_skelleton()
                doc['text'] = item['body']
                doc['created_at'] = item['created_at']
                doc['date'] = item['created_at']
                doc['id'] = item['id']
                doc['id_str'] = str(item['id'])

                if "symbols" in item:
                    for symbol in item['symbols']:
                        doc['entities']['symbols'].append({'indices':[],
                                                           'text':symbol['symbol']})

                if 'entities' in item:
                    if 'sentiment' in item['entities'] and item['entities']['sentiment']:
                        if 'basic' in item['entities']['sentiment']:
                            doc['user']['description'] = item['entities']['sentiment']['basic']

                doc['source'] = 'StockTwits'
                doc['constituent_name'] = constituent_name
                doc['constituent_id'] = constituent_id
                doc['search_term'] = ''
                doc["constituent"] = tah.get_old_constituent_name(constituent_id)
                doc['relevance'] = 1
                doc["sentiment_score"] = tah.get_nltk_sentiment(str(doc["text"]))
                tagged_text = tagger.get_spacy_entities(str(doc["text"]))
                doc["entity_tags"] = tah.get_spacey_tags(tagged_text)

                to_insert.append(doc)

            try:
                storage.insert_bigquery_data(common_parameters["BQ_DATASET"], parameters["DESTINATION_TABLE"], to_insert)
            except Exception as e:
                print(e)

            if parameters["LOGGING"]:
                doc = [{"date": time.strftime('%Y-%m-%d %H:%M:%S', datetime.now().date().timetuple()),
                        "constituent_name": constituent_name,
                        "constituent_id": constituent_id,
                        "downloaded_tweets": len(to_insert),
                        "language": 'StockTwits'}]
                logging_utils.logging(doc, common_parameters["BQ_DATASET"], parameters["LOGGING_TABLE"], storage)

        else:
            print(response.text)

def main(args):
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils import twitter_analytics_helpers as tah
    from utils.TaggingUtils import TaggingUtils as TU
    from utils import email_tools as email_tools

    param_table = "PARAM_STOCKTWITS_COLLECTION"
    parameters_list = ["LOGGING", "DESTINATION_TABLE", "LOGGING_TABLE"]

    parameters = tah.get_parameters(args.param_connection_string, param_table, parameters_list)

    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = tah.get_parameters(args.param_connection_string, common_table, common_list, common_where)

    try:
        get_stocktwits(args)
    except Exception as e:
        print(e)

    q1 = """
                    SELECT a.constituent_name, a.downloaded_tweets, a.date, a.language
                    FROM
                    (SELECT constituent_name, SUM(downloaded_tweets) as downloaded_tweets, DATE(date) as date, language
                    FROM `{0}.{1}`
                    WHERE language = 'StockTwits'
                    GROUP BY constituent_name, date, language
                    ) a,
                    (SELECT constituent_name, MAX(DATE(date)) as date
                    FROM `{0}.{1}`
                    WHERE language = 'StockTwits'
                    GROUP BY constituent_name
                    ) b
                    WHERE a.constituent_name = b.constituent_name AND a.date = b.date
                    GROUP BY a.constituent_name, a.downloaded_tweets, a.date, a.language;
                """.format(common_parameters["BQ_DATASET"],parameters["LOGGING_TABLE"])

    q2 = """
        SELECT constituent_name,count(*)
        FROM `{}.{}`
        WHERE source = 'StockTwits'
        GROUP BY constituent_name;
        """.format(common_parameters["BQ_DATASET"],parameters["LOGGING_TABLE"])

    email_tools.send_mail(args.param_connection_string, args.google_key_path, "StockTwits", "PARAM_STOCKTWITS_COLLECTION",
                  None,q1,q2)

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
    from utils import twitter_analytics_helpers as tah
    from utils.TaggingUtils import TaggingUtils as TU
    from utils.logging_utils import *
    from utils.email_tools import *
    main(args)

