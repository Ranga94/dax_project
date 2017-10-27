from sqlalchemy import *
from pymongo import MongoClient
from datetime import datetime
import time
import os
import smtplib
import sys
from bson.son import SON

def main(arguments):
    #os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = arguments.google_key_path

    #param_connection_string = "mysql+pymysql://igenie_readwrite:igenie@35.197.246.202/dax_project"
    param_connection_string = "mysql+pymysql://igenie_readwrite:igenie@127.0.0.1/dax_project"

    parameter_utility = ParameterUtils()

    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["LANGUAGE", "TWEETS_PER_QUERY",
                  "MAX_TWEETS", "CONNECTION_STRING",
                  "DATABASE_NAME", "COLLECTION_NAME",
                  "LOGGING_FLAG", "EMAIL_USERNAME",
                  "EMAIL_PASSWORD", "TWITTER_API_KEY",
                  "TWITTER_API_SECRET","BUCKET_NAME"]

    parameters = parameter_utility.get_parameters(sql_connection_string=param_connection_string,
                                                  sql_table_name=param_table,
                                                  sql_column_list=parameters_list)

    languages = parameters["LANGUAGE"].split(',')
    parameters["PARAM_CONNECTION_STRING"] = param_connection_string
    parameters["GOOGLE_KEY_PATH"] = arguments.google_key_path

    email_username = parameters.pop("EMAIL_USERNAME", None)
    email_pwd = parameters.pop("EMAIL_PASSWORD", None)

    for lang in languages:
        parameters["LANGUAGE"] = lang
        get_tweets(**parameters)

    #send_mail(parameters[3], arguments.param_connection_string)

def get_tweets(LANGUAGE, TWEETS_PER_QUERY, MAX_TWEETS, CONNECTION_STRING, DATABASE_NAME, COLLECTION_NAME,
               LOGGING_FLAG, TWITTER_API_KEY, TWITTER_API_SECRET, PARAM_CONNECTION_STRING, BUCKET_NAME,
               GOOGLE_KEY_PATH=None):
    storage = Storage()
    parameter_utility = ParameterUtils()

    downloader = TwitterDownloader(TWITTER_API_KEY, TWITTER_API_SECRET)
    downloader.load_api()

    #For now pass data_connection_string, but in reality it should be param_connection_string
    all_constituents = parameter_utility.get_param_data(sql_connection_string=PARAM_CONNECTION_STRING,
                                              sql_table_name="CONSTITUENTS_MASTER",
                                              sql_column_list=["CONSTITUENT_ID","NAME"])

    if LANGUAGE != "en":
        tweetsPerQry = 7

    for constituent_id, constituent_name in all_constituents:
        #For now, pass data_connection string. Later change it to param_connection_string
        search_query = get_search_string(constituent_id, CONNECTION_STRING, None, None)

        #Get max id of all tweets to extract tweets with id highe than that
        sinceId = get_max_id(constituent_name, CONNECTION_STRING, DATABASE_NAME, COLLECTION_NAME)
        max_id = -1
        tweetCount = 0

        #Set file name
        date = str(datetime.now().date())
        file_name = "{}_{}".format(date,constituent_name)
        cloud_file_name = "2017/{}".format(file_name)

        print("Downloading max {0} tweets for {1} in {2}".format(MAX_TWEETS, constituent_name, LANGUAGE))
        while tweetCount < MAX_TWEETS:
            tweets_to_save = []

            tweets, tmp_tweet_count, max_id = downloader.download(constituent_name, search_query,
                                                                  LANGUAGE,tweetsPerQry,sinceId,max_id)
            tweetCount += tmp_tweet_count

            #Add
            for tweet in tweets:
                tweet._json['date'] = datetime.strptime(tweet._json['created_at'], '%a %b %d %H:%M:%S %z %Y')
                tweet._json['constituent'] = constituent_name
                tweet._json['source'] = "Twitter"
                tweets_to_save.append(tweet._json)
                #tweet._json['constituent_name'] = constituent_name
                #tweet._json['constituent_id'] = constituent_id

            #Save to MongoDB
            storage.save_to_mongodb(CONNECTION_STRING, DATABASE_NAME, COLLECTION_NAME, tweets_to_save)

            #Save to local file
            if tweetCount == 0:
                storage.save_to_local_file(tweets_to_save, file_name, "w")
            else:
                storage.save_to_local_file(tweets_to_save, file_name, "a")

        #Upload file to cloud storage
        if os.path.isfile(file_name):
            if storage.upload_to_cloud_storage(GOOGLE_KEY_PATH, BUCKET_NAME, file_name, cloud_file_name):
                os.remove(file_name)
            else:
                print("File not uploaded to Cloud storage.")
        else:
            print("File does not exists in the local filesystem.")

        #logging
        if LOGGING_FLAG:
            pass
            logging(constituent_name, constituent_id, tweetCount, LANGUAGE,
                    CONNECTION_STRING, DATABASE_NAME)

    return "Downloaded tweets"

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

def get_search_string(constituent_id, connection_string, table_keywords, table_exclusions):
    ''''
    parameter_utility = ParameterUtils()

    where = lambda x: and_((x["ACTIVE_STATE"] == 1),(x["CONSTITUENT_ID"] == constituent_id))

    keywords = parameter_utility.get_param_data(sql_connection_string=connection_string,
                                                  sql_table_name=table_keywords,
                                                  sql_column_list=["KEYWORD"],
                                                sql_where=where)

    keywords_list = [key[0] for key in keywords]

    exclusions = parameter_utility.get_param_data(sql_connection_string=connection_string,
                                                sql_table_name=table_exclusions,
                                                sql_column_list=["EXCLUSION"],
                                                sql_where=where)

    exclusions_list = ["-" + key[0] for key in exclusions]

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
    from utils.ParameterUtils import ParameterUtils
    main(args)
