import sys
import pandas as pd
from pymongo import MongoClient
import os
import json

def other_tables(args):
    mongo_connection_string = "mongodb://igenie_readwrite:igenie@35.197.222.38:27017,35.197.238.147:27017,35.189.111.215:27017/dax_gcp?replicaSet=rs0"
    storage = Storage()
    client = MongoClient(mongo_connection_string)
    db = client["dax_gcp"]

    #tables = ["ADS_cor", "BMW_cor", "CB_cor", "DB_cor", "EON_cor"]
    tables = ["historical"]
    bucket_name = "igenie-financial"

    for collection_name in tables:
        collection = db[collection_name]

        cursor = collection.find({})
        data = list(cursor)
        file_name = "{}.json".format(collection_name)

        storage.save_to_local_file(data, file_name)

        cloud_file_name = "price_data/{}".format(file_name)

        if os.path.isfile(file_name):
            if storage.upload_to_cloud_storage(args.google_key_path, bucket_name, file_name, cloud_file_name):
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

    cursor = collection.find({}, no_cursor_timeout=True)

    print("Writing local file")
    with open(file_name, "w") as f:
        for tweet in cursor:
            id, name = tap.get_constituent_id_name(tweet["constituent"])
            tweet["constituent_name"] = name
            tweet["constituent_id"] = id
            tweet.pop("date")
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

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage, MongoEncoder
    from utils import twitter_analytics_helpers as tap
    #other_tables(args)
    tweet_table(args)
