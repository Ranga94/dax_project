from sqlalchemy import *
from datetime import datetime
import sys
from copy import deepcopy
import time

def main(args):
    sys.path.insert(0, args.python_path)
    from utils.TwitterDownloader import TwitterDownloader
    from utils.Storage import Storage
    from utils import twitter_analytics_helpers as tap
    from utils.TaggingUtils import TaggingUtils as TU
    from utils import email_tools as email_tools

    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["LANGUAGE", "TWEETS_PER_QUERY",
                       "MAX_TWEETS", "CONNECTION_STRING",
                       "DATABASE_NAME", "COLLECTION_NAME",
                       "LOGGING", "EMAIL_USERNAME",
                       "EMAIL_PASSWORD", "TWITTER_API_KEY",
                       "TWITTER_API_SECRET", "BUCKET_NAME","DESTINATION_TABLE","LOGGING_TABLE"]

    parameters = tap.get_parameters(args.param_connection_string, param_table, parameters_list)

    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = tap.get_parameters(args.param_connection_string, common_table, common_list, common_where)

    try:
        get_tweets(args)
    except Exception as e:
        print(e)

    q1 = """
                    SELECT a.constituent_name, a.downloaded_tweets, a.date, a.language
                    FROM
                    (SELECT constituent_name, SUM(downloaded_tweets) as downloaded_tweets, DATE(date) as date, language
                    FROM `{0}.{1}`
                    where language != 'StockTwits'
                    GROUP BY constituent_name, date, language
                    ) a,
                    (SELECT constituent_name, MAX(DATE(date)) as date
                    FROM `{0}.{1}`
                    WHERE language != 'StockTwits'
                    GROUP BY constituent_name
                    ) b
                    WHERE a.constituent_name = b.constituent_name AND a.date = b.date AND a.language = "en"
                    GROUP BY a.constituent_name, a.downloaded_tweets, a.date, a.language;
                """.format(common_parameters["BQ_DATASET"],parameters["LOGGING_TABLE"])

    q2 = """
        SELECT constituent_name,count(*)
        FROM `{}.{}`
        GROUP BY constituent_name;
    """.format(common_parameters["BQ_DATASET"],parameters["LOGGING_TABLE"])

    email_tools.send_mail(args.param_connection_string, args.google_key_path, "Twitter", "PARAM_TWITTER_COLLECTION",
                  None,q1,q2)

def get_tweets(args):
    from utils import logging_utils as logging_utils
    from utils.TwitterDownloader import TwitterDownloader
    from utils.Storage import Storage
    from utils import twitter_analytics_helpers as tap
    from utils.TaggingUtils import TaggingUtils as TU

    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["LANGUAGE", "TWEETS_PER_QUERY",
                       "MAX_TWEETS", "CONNECTION_STRING",
                       "DATABASE_NAME", "COLLECTION_NAME",
                       "LOGGING", "EMAIL_USERNAME",
                       "EMAIL_PASSWORD", "TWITTER_API_KEY",
                       "TWITTER_API_SECRET", "BUCKET_NAME","DESTINATION_TABLE","LOGGING_TABLE"]

    parameters = tap.get_parameters(args.param_connection_string, param_table, parameters_list)

    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = tap.get_parameters(args.param_connection_string, common_table, common_list, common_where)

    languages = parameters["LANGUAGE"].split(",")

    storage = Storage(google_key_path=args.google_key_path, mongo_connection_string=parameters["CONNECTION_STRING"])
    tagger = TU()

    downloader = TwitterDownloader(parameters["TWITTER_API_KEY"], parameters["TWITTER_API_SECRET"])
    downloader.load_api()

    all_constituents = storage.get_sql_data(sql_connection_string=args.param_connection_string,
                                              sql_table_name="MASTER_CONSTITUENTS",
                                              sql_column_list=["CONSTITUENT_ID","CONSTITUENT_NAME"])

    fields_to_keep = ["text", "favorite_count", "source", "retweeted","entities", "id_str",
                      "retweet_count","favorited","user","lang","created_at","place", "constituent_name",
                      "constituent_id", "search_term", "id", "sentiment_score", "entity_tags","relevance",
                      "constituent"]

    for language in languages:
        for constituent_id, constituent_name in all_constituents:
            search_query = get_search_string(constituent_id, args.param_connection_string, "PARAM_TWITTER_KEYWORDS",
                                             "PARAM_TWITTER_EXCLUSIONS")

            #Get max id of all tweets to extract tweets with id highe than that
            q = "SELECT MAX(id) as max_id FROM `{}.{}` WHERE constituent_id = '{}' " \
                "AND lang = '{}';".format(common_parameters["BQ_DATASET"],parameters["DESTINATION_TABLE"],
                                        constituent_id,language)
            try:
                sinceId =  int(storage.get_bigquery_data(q,iterator_flag=False)[0]["max_id"])
            except Exception as e:
                print(e)
                sinceId = None

            max_id = -1
            tweetCount = 0

            print("Downloading max {0} tweets for {1} in {2} on {3}".format(parameters["MAX_TWEETS"], constituent_name, language, str(datetime.now())))
            while tweetCount < parameters["MAX_TWEETS"]:
                tweets_unmodified = []
                tweets_modified = []
                tweets_mongo = []

                try:
                    tweets, tmp_tweet_count, max_id = downloader.download(constituent_name, search_query,
                                                                          language, parameters["TWEETS_PER_QUERY"], sinceId, max_id)
                except Exception as e:
                    continue

                if not tweets:
                    break
                else:
                    print("Downloaded {} tweets".format(tmp_tweet_count))

                tweetCount += tmp_tweet_count

                #Add fields for both unmodified and modified tweets
                for tweet in tweets:
                    tweet._json['source'] = "Twitter"
                    tweet._json['constituent_name'] = constituent_name
                    tweet._json['constituent_id'] = constituent_id
                    tweet._json['search_term'] = search_query
                    tweet._json["constituent"] = tap.get_old_constituent_name(constituent_id)

                    #Removing bad fields
                    clean_tweet = tap.scrub(tweet._json)

                    # Separate the tweets that go to one topic or the other

                    #unmodified
                    t_unmodified = deepcopy(clean_tweet)
                    t_unmodified["date"] = tap.convert_timestamp(t_unmodified["created_at"])
                    tweets_unmodified.append(t_unmodified)

                    #Add additional fields
                    clean_tweet["sentiment_score"] = tap.get_nltk_sentiment(str(clean_tweet["text"]))
                    tagged_text = tagger.get_spacy_entities(str(clean_tweet["text"]))
                    clean_tweet["entity_tags"] = tap.get_spacey_tags(tagged_text)
                    clean_tweet["relevance"] = -1

                    #mongo
                    t_mongo = deepcopy(clean_tweet)
                    t_mongo['date'] = datetime.strptime(t_mongo['created_at'], '%a %b %d %H:%M:%S %z %Y')
                    tweets_mongo.append(t_mongo)

                    #modified
                    tagged_tweet = dict((k,clean_tweet[k]) for k in fields_to_keep if k in clean_tweet)
                    tagged_tweet['date'] = tap.convert_timestamp(clean_tweet["created_at"])
                    tweets_modified.append(tagged_tweet)

                #send to PubSub topic
                #ps_utils.publish("igenie-project", "tweets-unmodified", tweets_unmodified)
                #ps_utils.publish("igenie-project", "tweets", tweets_modified)
                try:
                    storage.insert_bigquery_data(common_parameters["BQ_DATASET"],
                                                 '{}_unmodified'.format(parameters["DESTINATION_TABLE"]), tweets_unmodified)
                except Exception as e:
                    print(e)
                try:
                    storage.insert_bigquery_data(common_parameters["BQ_DATASET"], parameters["DESTINATION_TABLE"], tweets_modified)
                except Exception as e:
                    print(e)
                try:
                    storage.save_to_mongodb(tweets_mongo, "dax_gcp", parameters["DESTINATION_TABLE"])
                    pass
                except Exception as e:
                    print(e)

                time.sleep(1)

            print("Saved {} tweets for in {}".format(tweetCount, constituent_name,language))

            if parameters["LOGGING"]:
                doc = [{"date": time.strftime('%Y-%m-%d %H:%M:%S', datetime.now().date().timetuple()),
                        "constituent_name": constituent_name,
                        "constituent_id": constituent_id,
                        "downloaded_tweets": tweetCount,
                        "language": language}]
                logging_utils.logging(doc, common_parameters["BQ_DATASET"], parameters["LOGGING_TABLE"], storage)

    return "Downloaded tweets"

def get_search_string(constituent_id, connection_string, table_keywords, table_exclusions):
    if __name__ != "__main__":
        from utils.TwitterDownloader import TwitterDownloader
        from utils.Storage import Storage
        from utils import twitter_analytics_helpers as tap
        from utils.TaggingUtils import TaggingUtils as TU


    storage = Storage()

    where = lambda x: and_((x["ACTIVE_STATE"] == 1),(x["CONSTITUENT_ID"] == constituent_id))

    keywords = storage.get_sql_data(sql_connection_string=connection_string,
                                                  sql_table_name=table_keywords,
                                                  sql_column_list=["KEYWORD"],
                                                sql_where=where)

    keywords_list = ['"' + key[0] + '"' for key in keywords]
    keywords_string = " OR ".join(keywords_list)

    exclusions = storage.get_sql_data(sql_connection_string=connection_string,
                                                sql_table_name=table_exclusions,
                                                sql_column_list=["EXCLUSIONS"],
                                                sql_where=where)

    exclusions_list = ["-" + key[0] for key in exclusions]
    exclusions_string = " ".join(exclusions_list)

    all_words = keywords_string + exclusions_string

    return all_words

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    parser.add_argument('environment', help='production or test')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.TwitterDownloader import TwitterDownloader
    from utils.Storage import Storage
    from utils import twitter_analytics_helpers as tap
    from utils.TaggingUtils import TaggingUtils as TU
    from utils.logging_utils import *
    from utils.email_tools import *
    main(args)


