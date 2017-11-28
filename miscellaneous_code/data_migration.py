import sys
from pymongo import MongoClient
import os
import json
from pprint import pprint

def other_tables(args):
    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["CONNECTION_STRING"]

    parameters = get_parameters(args.param_connection_string, param_table, parameters_list)

    storage = Storage(google_key_path=args.google_key_path, mongo_connection_string=parameters["CONNECTION_STRING"])

    mongo_connection_string = parameters["CONNECTION_STRING"]


    client = MongoClient(mongo_connection_string)
    db = client["dax_gcp"]

    for collection_name in args.mongo_collections.split(","):
        collection = db[collection_name]
        cursor = collection.find({},{"_id":0})
        data = list(cursor)
        file_name = "{}.json".format(collection_name)

        open(file_name, 'w').write("\n".join(json.dumps(e, cls=MongoEncoder) for e in data))

        cloud_file_name = "{}/{}".format(args.bucket,file_name)

        if os.path.isfile(file_name):
            if storage.upload_to_cloud_storage(args.google_key_path, args.bucket, file_name, file_name):
                print("File uploaded to Cloud Storage")
                os.remove(file_name)
            else:
                print("File not uploaded to Cloud storage.")
        else:
            print("File does not exists in the local filesystem.")

def tweet_table(args):
    mongo_connection_string = "mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp"
    client = MongoClient(mongo_connection_string)
    db = client["dax_gcp"]
    collection = db["tweets"]
    file_name = "{}.json".format("tweets-raw")
    fields_to_keep = ["text", "favorite_count", "source", "retweeted", "entities", "id_str",
                      "retweet_count", "favorited", "user", "lang", "created_at", "place", "constituent_name",
                      "constituent_id", "search_term", "id", "sentiment_score", "entity_tags", "relevance"]

    #set which types of entities I want
    #set which types of user I want
    #set which types of place I want

    cursor = collection.find({}, no_cursor_timeout=True)


    print("Writing local file")
    with open(file_name, "w") as f:
        for tweet in cursor:
            scrubbed_tweet = tap.scrub(tweet)
            clean_tweet = dict((k, scrubbed_tweet[k]) for k in fields_to_keep if k in scrubbed_tweet)
            clean_tweet["date"] = tap.convert_timestamp(tweet["created_at"])

            f.write(json.dumps(tweet, cls=MongoEncoder) + '\n')

    return
    bucket_name = "igenie-tweets"
    cloud_file_name = "historical/{}".format(file_name)

    print("Writing to cloud storage")
    if os.path.isfile(file_name):
        if storage.upload_to_cloud_storage(args.google_key_path, bucket_name, file_name, cloud_file_name):
            print("File uploaded to Cloud Storage")
            #os.remove(file_name)
        else:
            print("File not uploaded to Cloud storage.")
    else:
        print("File does not exists in the local filesystem.")

def get_parameters(connection_string, table, column_list):
    storage = Storage()

    data = storage.get_sql_data(connection_string, table, column_list)[0]
    parameters = {}

    for i in range(0, len(column_list)):
        parameters[column_list[i]] = data[i]

    return parameters

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    parser.add_argument('mongo_collections', help='Comma separated list of collection names')
    parser.add_argument('bucket')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage, MongoEncoder
    from utils import twitter_analytics_helpers as tap
    other_tables(args)
