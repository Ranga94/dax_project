from sqlalchemy import *
from pymongo import MongoClient, errors
import tweepy
from datetime import datetime
import time
import os
import smtplib
import sys
import jsonpickle
from bson.son import SON
from google.cloud import storage
from . import TwitterDownloader
from . import Storage
from . import ParameterUtils

def main(arguments):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = arguments.google_key_path

    param_connection_string = "mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project"

    parameter_utility = ParameterUtils()

    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["LANGUAGE", "TWEETS_PER_QUERY",
                  "MAX_TWEETS", "CONNECTION_STRING",
                  "DATABASE_NAME", "COLLECTION_NAME",
                  "LOGGING_FLAG", "EMAIL_USERNAME",
                  "EMAIL_PASSWORD", "TWITTER_API_KEY",
                  "TWITTER_API_SECRET"]

    parameters = parameter_utility.get_parameters(sql_connection_string="mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project",
                                                  sql_table_name=param_table,
                                                  sql_column_list=parameters_list)

    languages = parameters["LANGUAGE"].split(',')
    parameters["PARAM_CONNECTION_STRING"] = param_connection_string

    print(parameters)
    return

    for lang in languages:
        parameters["LANGUAGE"] = lang
        get_tweets(**parameters)

    send_mail(parameters[3], arguments.param_connection_string)

def get_tweets(language, tweetsPerQry, maxTweets, data_connection_string, database, collection, logging_flag, param_connection_string):
    storage = Storage()


    api_key = ""
    api_secret = ""
    google_key_path = ""
    bucket_name = ""

    downloader = TwitterDownloader(api_key, api_secret)
    downloader.load_api()

    #For now pass data_connection_string, but in reality it should be param_connection_string
    all_constituents = get_constituents(data_connection_string)

    if language != "en":
        tweetsPerQry = 7

    for constituent_id, constituent_name in all_constituents:
        #For now, pass data_connection string. Later change it to param_connection_string
        searchQuery = get_search_string(constituent_id, data_connection_string)

        #Get max id of all tweets to extract tweets with id highe than that
        sinceId = get_max_id(constituent_name,data_connection_string,database,collection)
        max_id = -1
        tweetCount = 0

        #Set file name
        date = str(datetime.now().date())
        file_name = "{}_{}".format(date,constituent_name)
        cloud_file_name = "2017/{}".format(file_name)

        print("Downloading max {0} tweets for {1} in {2}".format(maxTweets, constituent_name, language))
        while tweetCount < maxTweets:
            tweets_to_save = []

            tweets, tmp_tweet_count, max_id = downloader.download(constituent_name, searchQuery,
                                                                  language,tweetsPerQry,sinceId,max_id)
            tweetCount += tmp_tweet_count

            #Add
            for tweet in tweets:
                tweet._json['date'] = datetime.strptime(tweet._json['created_at'], '%a %b %d %H:%M:%S %z %Y')
                tweet._json['constituent'] = constituent_name
                tweets_to_save.append(tweet._json)
                #tweet._json['constituent_name'] = constituent_name
                #tweet._json['constituent_id'] = constituent_id

            #Save to MongoDB
            storage.save_to_mongodb(data_connection_string, database, collection, tweets_to_save)

            #Save to local file
            if tweetCount == 0:
                storage.save_to_local_file(tweets_to_save, file_name, "w")
            else:
                storage.save_to_local_file(tweets_to_save, file_name, "a")

        #Upload file to cloud storage
        if os.path.isfile(file_name):
            if storage.upload_to_cloud_storage(google_key_path, bucket_name, file_name, cloud_file_name):
                os.remove(file_name)
            else:
                print("File not uploaded to Cloud storage.")
        else:
            print("File does not exists in the local filesystem.")

        #logging
        if logging_flag:
            pass
            logging(constituent_name, constituent_id, tweetCount, language, data_connection_string, database)

    return "Downloaded tweets"

def save_tweets(constituent_name, tweets, connection_string=None, database=None, filename=None):
    client = MongoClient(connection_string)
    db = client[database]
    collection = db["tweets_test"]

    try:
        result = collection.insert_many(tweets, ordered=False)
        print(result)
    except errors.BulkWriteError as e:
        print(str(e.details['writeErrors']))
        result = None
    except Exception as e:
        print(str(e))
        result = None

def logging(constituent_name, constituent_id, tweetCount, language, connection_string, database):
    client = MongoClient(connection_string)
    db = client[database]
    collection = db["tweet_logs"]

    doc = {"date":datetime.now(),
           "constituent_name":constituent_name,
           "constituent_id":constituent_id,
           "downloaded_tweets":tweetCount,
           "language":language}

    try:
        collection.insert_one(doc)
    except Exception as e:
        print("some error : " + str(e))

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

def get_constituents(connection_string):
    ''''
    engine = create_engine(connection_string)
    metadata = MetaData(engine)

    constituents_master = Table('CONSTITUENTS_MASTER', metadata, autoload=True)
    statement = select([constituents_master.c.CONSTITUENT_ID, constituents_master.c.NAME]).where(
            constituents_master.c.ACTIVE_STATE == 1)
    result = statement.execute()
    rows = result.fetchall()
    result.close()
    return rows
    '''

    result = []

    current_constituent = ['BMW', 'adidas', 'Deutsche Bank', 'EON', 'Commerzbank']

    for item in current_constituent:
        result.append((item, item))

    return result

def get_search_string(constituent_id, connection_string):
    ''''
    engine = create_engine(connection_string)
    metadata = MetaData(engine)
    keywords_table = Table('PARAM_TWITTER_KEYWORDS', metadata, autoload=True)
    statement = select([keywords_table.c.KEYWORD]).where((keywords_table.c.ACTIVE_STATE == 1) &
                                                         (keywords_table.c.CONSTITUENT_ID == constituent_id))
    result = statement.execute()
    keywords = result.fetchall()
    keywords_list = [key[0] for key in keywords]

    exclusions_table = Table('PARAM_TWITTER_EXCLUSIONS', metadata, autoload=True)
    statement = select([exclusions_table.c.EXCLUSIONS],
                       (exclusions_table.c.ACTIVE_STATE == 1) & (exclusions_table.c.CONSTITUENT_ID == constituent_id))
    result = statement.execute()
    exclusions = result.fetchall()
    exclusions_list = ["-" + key[0] for key in exclusions]

    result.close()

    all_words = keywords_list + exclusions_list

    return " ".join(all_words)
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

def get_parameters(sql_connection_string):
    engine = create_engine(sql_connection_string)
    metadata = MetaData(engine)

    param_twitter_collection = Table('PARAM_TWITTER_COLLECTION', metadata, autoload=True)
    statement = select([param_twitter_collection.c.LANGUAGE,
                        param_twitter_collection.c.TWEETS_PER_QUERY,
                        param_twitter_collection.c.MAX_TWEETS,
                        param_twitter_collection.c.CONNECTION_STRING,
                        param_twitter_collection.c.DATABASE_NAME,
                        param_twitter_collection.c.COLLECTION_NAME,
                        param_twitter_collection.c.LOGGING_FLAG])
    result = statement.execute()
    row = result.fetchone()
    result.close()
    return row

def load_api(sql_connection_string):
    engine = create_engine(sql_connection_string)
    metadata = MetaData(engine)

    param_twitter_collection = Table('PARAM_TWITTER_COLLECTION', metadata, autoload=True)
    statement = select([param_twitter_collection.c.TWITTER_API_KEY, param_twitter_collection.c.TWITTER_API_SECRET])
    # statement = param_twitter_collection.select([param_twitter_collection.c.TWITTER_API_KEY,
    #                                            param_twitter_collection.c.TWITTER_API_SECRET])
    result = statement.execute()
    api_key, api_secret = result.fetchone()
    result.close()

    auth = tweepy.AppAuthHandler(api_key, api_secret)

    api = tweepy.API(auth, wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)

    if (not api):
        print("Can't Authenticate")
        sys.exit(-1)
    else:
        return api

def send_mail(data_connection_string, param_connection_string):
    engine = create_engine(param_connection_string)
    metadata = MetaData(engine)

    param_twitter_collection = Table('PARAM_TWITTER_COLLECTION', metadata, autoload=True)
    statement = select([param_twitter_collection.c.EMAIL_USERNAME,
                        param_twitter_collection.c.EMAIL_PASSWORD])
    result = statement.execute()
    row = result.fetchone()
    result.close()

    fromaddr = row[0]

    client = MongoClient(data_connection_string)
    db = client["dax_gcp"]
    logging_collection = db['tweet_logs']

    daily = list(logging_collection.aggregate([
        {
            "$match": {
                "constituent_name": {"$exists": True}
            }
        },
        {
            "$project": {
                "constituent_name": 1,
                "day": {"$dayOfYear": "$date"},
                "downloaded_tweets": 1
            }
        },
        {
            "$group": {
                "_id": {"constituent_name": "$constituent_name", "day": "$day"},
                "tweets": {"$sum": "$downloaded_tweets"}
            }
        },
        {
            "$project": {
                "constituent": "$_id.constituent_name",
                "day": "$_id.day",
                "tweets": 1,
                "_id": 0
            }
        },
        {"$sort": SON([("day", -1)])}
    ]))

    day = daily[0]["day"]
    i = 0
    for item in daily:
        if item["day"] != day:
            break
        i += 1

    body = "Tweets collected today\n" + str(result[:i])
    subject = "Twitter collection logs: {}".format(time.strftime("%d/%m/%Y"))

    message = 'Subject: {}\n\n{}'.format(subject, body)

    # Credentials (if needed)
    username = row[0]
    password = row[1]

    toaddrs = ["ulysses@igenieconsulting.com", "twitter@igenieconsulting.com"]

    # The actual mail send
    server = smtplib.SMTP('smtp.gmail.com:587')
    server.starttls()
    server.login(username, password)
    server.sendmail(fromaddr, toaddrs, message)
    server.quit()--host

def save_to_cloud_storage(file_path):
    client = storage.Client()
    # The name for the new bucket
    bucket_name = 'igenie-ma-deals'
    bucket = client.get_bucket(bucket_name)

    blob = bucket.blob('2017/{}-BMW.json'.format(d))
    blob.upload_from_filename("./tweets.json")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    args = parser.parse_args()
    main(args)