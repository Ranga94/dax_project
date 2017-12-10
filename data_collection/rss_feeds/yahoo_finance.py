import feedparser
import sys
from pprint import pprint
from time import mktime
from datetime import datetime


def get_rss_feed(args):
    storage_client = Storage.Storage(google_key_path=args.google_key_path)

    # Get parameters
    param_table = "PARAM_NEWS_COLLECTION"
    parameters_list = ["LOGGING"]
    where = lambda x: x["SOURCE"] == 'RSS'

    parameters = get_parameters(args.param_connection_string, param_table, parameters_list, where)

    # get constituents
    all_constituents = storage_client.get_sql_data(sql_connection_string=args.param_connection_string,
                                                   sql_table_name="MASTER_CONSTITUENTS",
                                                   sql_column_list=["CONSTITUENT_ID",
                                                                    "CONSTITUENT_NAME",
                                                                    "SYMBOL"])

    for constituent_id, constituent_name, symbol in all_constituents:
        # get last news date for the constituent
        query = """
                    SELECT max(news_date) as max_date FROM `pecten_dataset.all_news`
                    WHERE constituent_id = '{}' and news_origin = "Yahoo Finance RSS"
                    """.format(constituent_id)

        try:
            result = storage_client.get_bigquery_data(query=query, iterator_flag=False)
            max_date = result[0]["max_date"]
        except Exception as e:
            max_date = None
            continue

        print("Getting RSS Feed for {}".format(constituent_name))
        d = feedparser.parse('http://finance.yahoo.com/rss/headline?s={}'.format(symbol + ".DE"))

        to_insert = []

        for post in d.entries:
            date = datetime.fromtimestamp(mktime(post.published_parsed))
            if max_date and date < max_date:
                continue
            else:
                doc = {"url": post.link, "news_date": date.strftime('%Y-%m-%d %H:%M:%S'),
                       "news_language":post.summary_detail["language"],
                       "news_title":post.title, "news_article_txt":post.summary, "news_origin":"Yahoo Finance RSS",
                       "show":True}

                # Get sentiment score
                doc["score"] = get_nltk_sentiment(str(doc["news_article_txt"]))

                # get sentiment word
                doc["sentiment"] = get_sentiment_word(doc["score"])

                # add constituent name, id and old name
                doc["constituent_id"] = constituent_id
                doc["constituent_name"] = constituent_name
                old_constituent_name = get_old_constituent_name(constituent_id)
                doc["constituent"] = old_constituent_name

                to_insert.append(doc)

        try:
            print("Inserting into BQ")
            storage_client.insert_bigquery_data("pecten_dataset", "all_news", to_insert)
        except Exception as e:
            print(e)

        log = [{"date": datetime.now().date().strftime('%Y-%m-%d %H:%M:%S'),
                "constituent_name": constituent_name,
                "constituent_id": constituent_id,
                "downloaded_news": len(to_insert),
                "source": "Yahoo Finance RSS"}]

        if parameters["LOGGING"] and to_insert:
            logging(log, 'pecten_dataset', "news_logs", storage_client)

        break


def main(args):
    get_rss_feed(args)

    q1 = """
                        SELECT a.constituent_name, a.downloaded_news, a.date, a.source
                        FROM
                        (SELECT constituent_name, SUM(downloaded_news) as downloaded_news, DATE(date) as date, source
                        FROM `pecten_dataset.news_logs`
                        WHERE source = 'Yahoo Finance RSS'
                        GROUP BY constituent_name, date, source
                        ) a,
                        (SELECT constituent_name, MAX(DATE(date)) as date
                        FROM `igenie-project.pecten_dataset.news_logs`
                        WHERE source = 'Yahoo Finance RSS'
                        GROUP BY constituent_name
                        ) b
                        WHERE a.constituent_name = b.constituent_name AND a.date = b.date
                        GROUP BY a.constituent_name, a.downloaded_news, a.date, a.source;
                """

    q2 = """
                            SELECT constituent_name,count(*)
                            FROM `pecten_dataset.all_news`
                            WHERE news_origin = "Yahoo Finance RSS"
                            GROUP BY constituent_name
        """

    print("Sending email")
    send_mail(args.param_connection_string, args.google_key_path, "RSS Feed",
              "PARAM_NEWS_COLLECTION", lambda x: x["SOURCE"] == "RSS", q1, q2)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The MySQL connection string')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils.twitter_analytics_helpers import *
    from utils.TaggingUtils import TaggingUtils as TU
    from utils.logging_utils import *
    from utils.email_tools import *
    main(args)
