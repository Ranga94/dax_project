from sqlalchemy import *
from datetime import datetime
import time
import smtplib
import sys
from bson.son import SON
from pymongo import MongoClient
from copy import deepcopy
from pprint import pprint, pformat

def main(arguments):
    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["LANGUAGE", "TWEETS_PER_QUERY",
                  "MAX_TWEETS", "CONNECTION_STRING",
                  "DATABASE_NAME", "COLLECTION_NAME",
                  "LOGGING_FLAG", "EMAIL_USERNAME",
                  "EMAIL_PASSWORD", "TWITTER_API_KEY",
                  "TWITTER_API_SECRET","BUCKET_NAME"]

    parameters = get_parameters(arguments.param_connection_string, param_table, parameters_list)

    languages = parameters["LANGUAGE"].split(',')
    parameters["PARAM_CONNECTION_STRING"] = arguments.param_connection_string
    parameters["GOOGLE_KEY_PATH"] = arguments.google_key_path

    email_username = parameters.pop("EMAIL_USERNAME", None)
    email_pwd = parameters.pop("EMAIL_PASSWORD", None)

    for lang in languages:
        parameters["LANGUAGE"] = lang
        get_tweets(**parameters)

    if parameters["LOGGING_FLAG"]:
        send_mail(arguments.param_connection_string, arguments.google_key_path)

def get_parameters(connection_string, table, column_list):
    storage = Storage()

    data = storage.get_sql_data(connection_string, table, column_list)[0]
    parameters = {}

    for i in range(0, len(column_list)):
        parameters[column_list[i]] = data[i]

    return parameters

def get_tweets(LANGUAGE, TWEETS_PER_QUERY, MAX_TWEETS, CONNECTION_STRING, DATABASE_NAME, COLLECTION_NAME,
               LOGGING_FLAG, TWITTER_API_KEY, TWITTER_API_SECRET, PARAM_CONNECTION_STRING, BUCKET_NAME,
               GOOGLE_KEY_PATH=None):

    storage = Storage(google_key_path=GOOGLE_KEY_PATH, mongo_connection_string=CONNECTION_STRING)
    tagger = TU()

    downloader = TwitterDownloader(TWITTER_API_KEY, TWITTER_API_SECRET)
    downloader.load_api()

    all_constituents = storage.get_sql_data(sql_connection_string=PARAM_CONNECTION_STRING,
                                              sql_table_name="MASTER_CONSTITUENTS",
                                              sql_column_list=["CONSTITUENT_ID","CONSTITUENT_NAME"])

    if LANGUAGE != "en":
        TWEETS_PER_QUERY = 7

    fields_to_keep = ["text", "favorite_count", "source", "retweeted","entities", "id_str",
                      "retweet_count","favorited","user","lang","created_at","place", "constituent_name",
                      "constituent_id", "search_term", "id", "sentiment_score", "entity_tags","relevance"]

    for constituent_id, constituent_name in all_constituents:
        search_query = get_search_string(constituent_id, PARAM_CONNECTION_STRING, "PARAM_TWITTER_KEYWORDS",
                                         "PARAM_TWITTER_EXCLUSIONS")

        #Get max id of all tweets to extract tweets with id highe than that
        q = "SELECT MAX(id) as max_id FROM `pecten_dataset.tweets` WHERE constituent_id = '{}';".format(constituent_id)
        try:
            sinceId =  int(storage.get_bigquery_data(q,iterator_flag=False)[0]["max_id"])
        except Exception as e:
            print(e)
            sinceId = None

        max_id = -1
        tweetCount = 0

        print("Downloading max {0} tweets for {1} in {2} on {3}".format(MAX_TWEETS, constituent_name, LANGUAGE, str(datetime.now())))
        while tweetCount < MAX_TWEETS:
            tweets_unmodified = []
            tweets_modified = []
            tweets_mongo = []

            try:
                tweets, tmp_tweet_count, max_id = downloader.download(constituent_name, search_query,
                                                                      LANGUAGE, TWEETS_PER_QUERY, sinceId, max_id)
            except Exception as e:
                continue


            print("Downloaded {} tweets".format(tmp_tweet_count))
            if not tweets:
                break

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
                storage.insert_bigquery_data('pecten_dataset', 'tweets_unmodified', tweets_unmodified)
            except Exception as e:
                print(e)
            try:
                storage.insert_bigquery_data('pecten_dataset', 'tweets', tweets_modified)
            except Exception as e:
                print(e)
            try:
                storage.save_to_mongodb(tweets_mongo, "dax_gcp", "tweets")
                pass
            except Exception as e:
                print(e)


            time.sleep(1)

        print("Saved {} tweets".format(tweetCount))

        if LOGGING_FLAG:
            logging(constituent_name, constituent_id, tweetCount, LANGUAGE,
                    'pecten_dataset', 'tweet_logs', storage)

    return "Downloaded tweets"

def logging(constituent_name, constituent_id, tweetCount, language, dataset_name, table_name, storage_object):
    doc = [{"date": time.strftime('%Y-%m-%d %H:%M:%S', datetime.now().date().timetuple()),
           "constituent_name": constituent_name,
           "constituent_id": constituent_id,
           "downloaded_tweets": tweetCount,
           "language": language}]

    try:
        storage_object.insert_bigquery_data(dataset_name, table_name, doc)
    except Exception as e:
        print(e)

    '''
    client = MongoClient(connection_string)
    db = client[database]
    collection = db["tweet_logs"]


    try:
        collection.insert_one(doc)
    except Exception as e:
        print("some error : " + str(e))
    '''

def get_max_id(constituent_id, connection_string, database, table):
    client = MongoClient(connection_string)
    db = client[database]
    collection = db[table]

    res = list(collection.aggregate([
        {
            "$match": {"constituent": constituent_id}
        },
        {
            "$group": {
                "_id": "$constituent",
                "max_id": {"$max": "$id"}
            }
        }
    ]))

    if not res:
        return None

    return res[0]["max_id"]

def get_search_string(constituent_id, connection_string, table_keywords, table_exclusions):
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
    '''
    client = MongoClient(connection_string)
    db = client["dax_gcp"]
    collection = db["constituents"]
    constituent_data = collection.find_one({'name': constituent_id})
    keywords = constituent_data['keywords']
    exclusions = constituent_data['exclusions']

    k = " OR ".join(keywords)
    ex = " ".join(exclusions)
    searchQuery = k + " " + ex


    return searchQuery
    '''

def send_mail(param_connection_string, google_key_path):
    storage = Storage(google_key_path=google_key_path)

    parameters = get_parameters(param_connection_string,"PARAM_TWITTER_COLLECTION",["EMAIL_USERNAME",
                                                                            "EMAIL_PASSWORD"])
    q1 = """
    SELECT a.constituent_name, a.downloaded_tweets, a.date, a.language
    FROM
    (SELECT constituent_name, SUM(downloaded_tweets) as downloaded_tweets, DATE(date) as date, language
    FROM `pecten_dataset.tweet_logs`
    GROUP BY constituent_name, date, language
    ) a,
    (SELECT constituent_name, MAX(DATE(date)) as date
    FROM `igenie-project.pecten_dataset.tweet_logs`
    GROUP BY constituent_name
    ) b
    WHERE a.constituent_name = b.constituent_name AND a.date = b.date AND a.language = "en"
    GROUP BY a.constituent_name, a.downloaded_tweets, a.date, a.language;
    """
    print(q1)

    latest_logs = storage.get_bigquery_data(query=q1,iterator_flag=False)
    latest_logs_list = [l.values() for l in latest_logs]

    q2 = """
    SELECT constituent_name,count(*)
    FROM `pecten_dataset.tweets`
    GROUP BY constituent_name;
    """

    total_tweets = storage.get_bigquery_data(query=q2, iterator_flag=False)
    total_tweets_list = [l.values() for l in total_tweets]


    body = "Latest tweets collected\n" + pformat(latest_logs_list) + "\n\n\n" + \
    "Total tweets\n" + pformat(total_tweets_list)
    subject = "Twitter collection logs: {}".format(time.strftime("%d/%m/%Y"))

    message = 'Subject: {}\n\n{}'.format(subject, body)

    # Credentials (if needed)
    username = parameters["EMAIL_USERNAME"]
    password = parameters["EMAIL_PASSWORD"]

    toaddrs = ["ulysses@igenieconsulting.com", "twitter@igenieconsulting.com"]

    # The actual mail send
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, password)
    server.sendmail(username, toaddrs, message)
    server.quit()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.TwitterDownloader import TwitterDownloader
    from utils.Storage import Storage
    from utils.PubsubUtils import PubsubUtils
    from utils import twitter_analytics_helpers as tap
    from utils.TaggingUtils import TaggingUtils as TU
    main(args)

