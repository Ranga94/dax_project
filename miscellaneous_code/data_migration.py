import sys
from pymongo import MongoClient
import os
import json
from pprint import pprint
from copy import deepcopy

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
    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["CONNECTION_STRING"]

    parameters = get_parameters(args.param_connection_string, param_table, parameters_list)

    mongo_connection_string = parameters["CONNECTION_STRING"]
    client = MongoClient(mongo_connection_string)
    db = client["dax_gcp"]
    collection = db["tweets"]
    file_name = "{}.json".format("tweets-raw")
    file_name_unmodified = "{}.json".format("tweets-unmodified")
    fields_to_keep = ["text", "favorite_count", "source", "retweeted", "entities", "id_str",
                      "retweet_count", "favorited", "user", "lang", "created_at", "place", "constituent_name",
                      "constituent_id", "search_term", "id", "sentiment_score", "entity_tags", "relevance"]


    cursor = collection.find({}, no_cursor_timeout=True)


    print("Writing local file")
    with open(file_name, "w") as f, open(file_name_unmodified, "w") as f2:
        count = 0
        for tweet in cursor:
            # Removing bad fields
            clean_tweet = tap.scrub(tweet)
            clean_tweet["constituent"]

            # Separate the tweets that go to one topic or the other

            # unmodified
            t_unmodified = deepcopy(clean_tweet)
            t_unmodified["date"] = tap.convert_timestamp(t_unmodified["created_at"])
            f2.write(json.dumps(t_unmodified, cls=MongoEncoder) + '\n')

            # modified
            tagged_tweet = dict((k, clean_tweet[k]) for k in fields_to_keep if k in clean_tweet)
            tagged_tweet['date'] = tap.convert_timestamp(clean_tweet["created_at"])
            f.write(json.dumps(tagged_tweet, cls=MongoEncoder) + '\n')
            count += 1

            if count == 10000:
                print("Saved {} tweets".format(count))
                count = 0

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

def twitter_table(self, d={}):
    list = ['text', 'favorite_count', 'source', 'retweeted', 'entities', 'entities.symbols',
            'entities.symbols.indices',
            'entities.symbols.text', 'entities.media', 'entities.media.source_status_id_str',
            'entities.media.expanded_url',
            'entities.media.display_url', 'entities.media.url', 'entities.media.media_url_https',
            'entities.media.source_status_id'
            'entities.media.video_info', 'entities.media.video_info.aspect_ratio',
            'entities.media.video_info.variants', 'entities.media.video_info.variants.url'
                                                  'entities.media.video_info.variants.bitrate',
            'entities.media.video_info.variants.content_type', 'entities.media.id_str', 'entities.media.sizes'
                                                                                        'entities.media.sizes.small',
            'entities.media.sizes.small.h', 'entities.media.sizes.small.resize', 'entities.media.sizes.small.w',
            'entities.media.sizes.large'
            'entities.media.sizes.large.h', 'entities.media.sizes.large.resize', 'entities.media.sizes.large.w',
            'entities.media.sizes.medium', 'entities.media.sizes.medium.h',
            'entities.media.sizes.medium.resize', 'entities.media.sizes.medium.w', 'entities.media.sizes.thumb',
            'entities.media.sizes.thumb.h', 'entities.media.sizes.thumb.resize',
            'entities.media.sizes.thumb.w', 'entities.media.indices', 'entities.media.type', 'entities.media.id',
            'entities.media.media_url', 'entities.hashtags',
            'entities.hashtags.indices', 'entities.hashtags.text', 'entities.user_mentions',
            'entities.user_mentions.id', 'entities.user_mentions.indices',
            'entities.user_mentions.id_str', 'entities.user_mentions.screen_name', 'entities.user_mentions.name',
            'entities.trends', 'entities.trends.woeid'
                               'entities.trends.name', 'entities.trends.countryCode', 'entities.trends.url',
            'entities.trends.country', 'entities.trends.placeType',
            'entities.trends.placeType.code', 'entities.trends.placeType.name', 'entities.trends.parentid',
            'entities.urls', 'entities.urls.url',
            'entities.urls.indices', 'entities.urls.expanded_url', 'entities.urls.display_url', 'id_str',
            'retweet_count', 'favorited', 'user', 'user.follow_request_sent',
            'user.profile_use_background_image', 'user.default_profile_image', 'user.id', 'user.verified',
            'user.profile_image_url_https', 'user.profile_sidebar_fill_color',
            'user.profile_text_color', 'user.followers_count', 'user.profile_sidebar_border_color', 'user.id_str',
            'user.profile_background_color', 'user.listed_count',
            'user.profile_background_image_url_https', 'user.utc_offset', 'user.statuses_count', 'user.description',
            'user.friends_count', 'user.location', 'user.profile_link_color',
            'user.profile_image_url', 'user.following', 'user.geo_enabled', 'user.profile_banner_url',
            'user.profile_background_image_url', 'user.name', 'user.lang',
            'user.profile_background_tile', 'user.favourites_count', 'user.screen_name', 'user.notifications',
            'user.url', 'user.created_at', 'user.contributors_enabled',
            'user.time_zone', 'user.protected', 'user.default_profile', 'user.is_translator', 'lang', 'created_at',
            'place', 'place.full_name', 'place.url', 'place.country',
            'place.place_type', 'place.bounding_box', 'place.bounding_box.type', 'place.bounding_box.coordinates',
            'place.bounding_box.coordinates.sw', 'place.bounding_box.coordinates.sw.lat',
            'place.bounding_box.coordinates.sw.long', 'place.bounding_box.coordinates.ne',
            'place.bounding_box.coordinates.ne.lat', 'place.bounding_box.coordinates.ne.long',
            'place.bounding_box.coordinates.se', 'place.bounding_box.coordinates.se.lat',
            'place.bounding_box.coordinates.se.long', 'place.bounding_box.coordinates.nw',
            'place.bounding_box.coordinates.nw.lat', 'place.bounding_box.coordinates.nw.long', 'place.country_code',
            'place.id', 'place.name', 'constituent_name', 'constituent_id',
            'search_term', 'relevance', 'date', 'sentiment_score', 'entity_tags', 'entity_tags.PERSON',
            'entity_tags.NORP', 'entity_tags.FACILITY', 'entity_tags.ORG', 'entity_tags.GPE',
            'entity_tags.LOC', 'entity_tags.PRODUCT', 'entity_tags.EVENT', 'entity_tags.WORK_OF_ART',
            'entity_tags.LAW', 'entity_tags.LANGUAGE', 'entity_tags.DATE', 'entity_tags.TIME',
            'entity_tags.PERCENT', 'entity_tags.MONEY', 'entity_tags.QUANTITY', 'entity_tags.ORDINAL',
            'entity_tags.CARDINAL', 'entity_tags.FAC', 'id']
    a = list(d.keys())
    b = list(d.values())
    for idx, elem in enumerate(a):
        for index, val in enumerate(list):
            z = 0
            if elem == val:
                z = 1
                break
        if z == 0:
            del a[idx]
            del b[idx]
    final_dict = dict(zip(a, b))
    return final_dict

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    parser.add_argument('function')
    parser.add_argument('mongo_collections', help='Comma separated list of collection names')
    parser.add_argument('bucket')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage, MongoEncoder
    from utils import twitter_analytics_helpers as tap
    if args.function == "other_tables":
        other_tables(args)
    elif args.function == "tweet_table":
        tweet_table(args)


