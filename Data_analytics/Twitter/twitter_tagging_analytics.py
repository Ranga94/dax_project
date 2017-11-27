import sys
from pymongo import MongoClient
from datetime import datetime
from bson.son import SON
from pprint import pprint

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

def main(args):
    from_date = datetime(2017, 10, 1)
    to_date = datetime(2017, 11, 16)
    get_twitter_analytics_top_orgs(args, from_date, to_date)



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
