import sys
from pymongo import MongoClient
from datetime import datetime
from bson.son import SON
from pprint import pprint
import pandas as pd
import json
import time

def get_twitter_analytics_top_orgs(args, from_date, to_date):
    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["CONNECTION_STRING"]

    parameters = get_parameters(args.param_connection_string, param_table, parameters_list)

    storage = Storage(google_key_path=args.google_key_path, mongo_connection_string=parameters["CONNECTION_STRING"])

    all_constituents = storage.get_sql_data(sql_connection_string=args.param_connection_string,
                                            sql_table_name="MASTER_CONSTITUENTS",
                                            sql_column_list=["CONSTITUENT_ID", "CONSTITUENT_NAME"])

    client = MongoClient(parameters["CONNECTION_STRING"])
    db = client["dax_gcp"]
    collection = db["tweets"]
    twitter_analytics_top_orgs = db["twitter_analytics_top_orgs"]

    for constituent_id, constituent_name in all_constituents:
        print(constituent_name)
        pipeline = [
            {
                "$match": {
                    "constituent_id": constituent_id,
                    "date": {"$gte": from_date, "$lte": to_date}
                }
            },
            {
                "$project": {
                    "constituent_id": "$constituent_id",
                    "constituent_name": "$constituent_name",
                    "org_tags": "$entity_tags.ORG",
                    "constituent": "$constituent"
                }
            },
            {"$unwind":"$org_tags"},
            {
                "$group":{
                    "_id": {"constituent_id": "$constituent_id", "org_tags": "$org_tags",
                            "constituent_name": "$constituent_name"},
                    "count": {"$sum": 1}
                }
            },
            {"$sort": SON([("count", -1)])},
            {
                "$project": {
                    "_id": 0,
                    "constituent_id": "$_id.constituent_id",
                    "constituent_name": "$_id.constituent_name",
                    "org_tags": "$_id.org_tags",
                    "count": "$count",
                    "from_date": from_date,
                    "to_date":to_date,
                    "date":datetime.now()
                }
            }
        ]

        results = list(collection.aggregate(pipeline))
        to_return = []

        for r in results:
            if r["constituent_name"].lower() in r["org_tags"].lower() or \
               r["org_tags"].lower() in r["constituent_name"].lower() or \
               " AG".lower() in r["org_tags"].lower() or \
               len(r["org_tags"].split(" ")) > 3 or \
               r["count"] < 10 or \
               r["org_tags"].lower() == "Trends".lower() or \
               r["org_tags"].lower() == "Share".lower():
                continue
            else:
                to_return.append(r)

        if to_return:
            twitter_analytics_top_orgs.insert_many(to_return)
        else:
            if results:
                twitter_analytics_top_orgs.insert_many(results)

def get_parameters(connection_string, table, column_list):
    storage = Storage()

    data = storage.get_sql_data(connection_string, table, column_list)[0]
    parameters = {}

    for i in range(0, len(column_list)):
        parameters[column_list[i]] = data[i]

    return parameters

def get_twitter_analytics_latest_price_tweets(args, from_date, to_date):
    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["CONNECTION_STRING"]

    parameters = get_parameters(args.param_connection_string, param_table, parameters_list)

    storage = Storage(google_key_path=args.google_key_path, mongo_connection_string=parameters["CONNECTION_STRING"])

    all_constituents = storage.get_sql_data(sql_connection_string=args.param_connection_string,
                                            sql_table_name="MASTER_CONSTITUENTS",
                                            sql_column_list=["CONSTITUENT_ID", "CONSTITUENT_NAME","SYMBOL"])

    client = MongoClient(parameters["CONNECTION_STRING"])
    db = client["dax_gcp"]
    collection = db["tweets"]
    twitter_analytics_latest_price_tweets = db["twitter_analytics_latest_price_tweets"]

    for constituent_id, constituent_name, symbol in all_constituents:
        symbol = '$' + symbol
        print(symbol)
        to_return = []
        final = []


        results = list(collection.find({"date":{"$gte": from_date, "$lte": to_date},
                                        "constituent_id":constituent_id,
                                        "entities.symbols":{"$exists":True, "$ne":[]}},
                                       {"text":1,"constituent_id":1,"constituent_name":1,
                                       "constituent":1,"entity_tags.MONEY":1,"sentiment_score":1,"date":1,"_id":0})
                       .sort([("date",-1)]))

        if results:
            print("A")
            for item in results:
                if symbol in item["text"].split(" "):
                    if item["entity_tags"]["MONEY"]:
                        text = item["text"].replace("$", "")
                        text = text.replace("€", "")
                        text = text.replace("EUR", " ")
                        tokens = text.split(" ")
                        contains_number = False
                        for t in tokens:
                            try:
                                float(t)
                                contains_number = True
                            except Exception as e:
                                pass

                        if contains_number:
                            clean_tokens = [t for t in item["text"].split(" ") if "http" not in t]
                            item["text"] = " ".join(clean_tokens)
                            item["tweet_date"] = item["date"]
                            item["date"] = datetime.now()
                            item["from_date"] = from_date
                            item["to_date"] = to_date
                            to_return.append(item)

            if to_return:
                df = pd.DataFrame(to_return)
                df.drop_duplicates("text",inplace=True)

                final = df.to_json(orient="records", date_format="iso")
                final = json.loads(final)

                for f in final:
                    f["from_date"] = datetime.strptime(f["from_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    f["to_date"] = datetime.strptime(f["to_date"], "%Y-%m-%dT%H:%M:%S.%fZ")
            else:
                print("A2")
                results = list(collection.find({"date": {"$gte": from_date, "$lte": to_date},
                                                "constituent_id": constituent_id,
                                                "user.followers_count": {"$gte": 200}},
                                               {"text": 1, "constituent_id": 1, "constituent_name": 1,
                                                "constituent": 1, "entity_tags.MONEY": 1, "sentiment_score": 1,
                                                "date": 1, "_id": 0})
                               .limit(5)
                               .sort([("date", -1)]))

                if results:
                    df = pd.DataFrame(results)
                    df.drop_duplicates("text", inplace=True)

                    final = df.to_json(orient="records", date_format="iso")
                    final = json.loads(final)

                    for f in final:
                        f["from_date"] = from_date
                        f["to_date"] = to_date
                else:
                    print("B2")
                    final = [{'constituent': constituent_name,
                              'constituent_id': constituent_id,
                              'constituent_name': constituent_name,
                              'date': datetime.now(),
                              'entity_tags': {'MONEY': ['']},
                              'from_date': from_date,
                              'sentiment_score': 0.0,
                              'text': 'No new tweets.',
                              'to_date': to_date,
                              'tweet_date': None}]



        else:
            print("B")
            results = list(collection.find({"date":{"$gte": from_date, "$lte": to_date},
                                        "constituent_id":constituent_id,
                                        "user.followers_count":{"$gte":200}},
                                       {"text":1,"constituent_id":1,"constituent_name":1,
                                       "constituent":1,"entity_tags.MONEY":1,"sentiment_score":1,"date":1,"_id":0})
                           .limit(5)
                       .sort([("date",-1)]))

            if results:
                df = pd.DataFrame(results)
                df.drop_duplicates("text", inplace=True)

                final = df.to_json(orient="records", date_format="iso")
                final = json.loads(final)

                for f in final:
                    f["from_date"] = from_date
                    f["to_date"] = to_date

            else:
                print("C")
                results = list(collection.find({"constituent_id": constituent_id},
                                               {"text": 1, "constituent_id": 1, "constituent_name": 1,
                                                "constituent": 1, "entity_tags.MONEY": 1, "sentiment_score": 1,
                                                "date": 1, "_id": 0})
                               .limit(5)
                               .sort([("date", -1)]))

                if results:
                    df = pd.DataFrame(results)
                    df.drop_duplicates("text", inplace=True)

                    final = df.to_json(orient="records", date_format="iso")
                    final = json.loads(final)

                    for f in final:
                        f["from_date"] = from_date
                        f["to_date"] = to_date

                else:
                    print("D")
                    final = [{'constituent': constituent_name,
                              'constituent_id': constituent_id,
                              'constituent_name': constituent_name,
                              'date': datetime.now(),
                              'entity_tags': {'MONEY': ['']},
                              'from_date': from_date,
                              'sentiment_score': 0.0,
                              'text': 'No new tweets.',
                              'to_date': to_date,
                              'tweet_date': None}]

        if final:
           twitter_analytics_latest_price_tweets.insert_many(final)
        time.sleep(1)

def main(args):
    from_date = datetime(2017, 10, 1)
    to_date = datetime(2017, 11, 16)
    #get_twitter_analytics_top_orgs(args, from_date, to_date)
    get_twitter_analytics_latest_price_tweets(args, from_date, to_date)



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils import twitter_analytics_helpers as tap
    main(args)
