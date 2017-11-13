import sys
import os
from google.cloud import bigquery
import datetime
import json

def update_from_cloud_storage(args):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.google_key_path
    client = bigquery.Client()

    bucket_name = "igenie-tweets"
    blob_name = "historical/{}.json".format("tweets-raw")

    GS_URL = 'gs://{}/{}'.format(bucket_name, blob_name)
    external_config = bigquery.ExternalConfig("NEWLINE_DELIMITED_JSON")
    external_config.autodetect = True
    external_config.source_uris = [GS_URL]
    job_config = bigquery.QueryJobConfig()
    job_config.table_definitions = {"temp": external_config}

    file_name = "tweets-enriched.json"

    QUERY = ('SELECT id,'
             'id_str,'
             'constituent,'
             'text,'
             'coordinates,'
             'created_at,'
             'favorited,'
             'place,'
             'lang,'
             'metadata,'
             'retweeted,'
             'entities.hashtags,'
             'entities.symbols,'
             'source,'
             'user.time_zone,'
             'user.location,'
             'user.friends_count,'
             'user.followers_count,'
             'favorite_count,'
             'retweet_count,'
             'geo ,'
             'search_term '
             'FROM `temp`')

    TIMEOUT = 100  # in seconds

    query_job = client.query(QUERY, job_config=job_config)  # API request - starts the query
    assert query_job.state == 'RUNNING'

    # Waits for the query to finish
    iterator = query_job.result(timeout=TIMEOUT)

    with open(file_name, "a") as f:
        for row in iterator:
            # Included attributes
            result = {}
            result["id"] = row.id
            result['id_str'] = row.id_str
            result['text'] = row.text
            result['coordinates'] = row.coordinates
            result['favorited'] = row.favorited
            result['place'] = row.place
            result['lang'] = row.lang
            result['metadata'] = row.metadata
            result['retweeted'] = row.retweeted
            result['entities_hashtags'] = row["entities.hashtags"]
            result['entities_symbols'] = row["entities.symbols"]
            result['source'] = row.source
            result['user_time_zone'] = row["user.time_zone"]
            result['user_location'] = row["user.location"]
            result['user_friends_count'] = row["user.friends_count"]
            result['user_followers_count'] = row["user.followers_count"]
            result['favorite_count'] = row.favorite_count
            result['retweet_count'] = row.retweet_count
            result['geo'] = row.geo
            result['search_term'] = row.search_term

            # Extra attributes
            # constituent_id, constituent_name
            constituent_id, constituent_name = get_constituent_id_name(row.constituent)
            result['constituent_id'] = constituent_id
            result['constituent_name'] = constituent_name
            # created at - date
            result['date'] = datetime.strptime(row.created_at, '%a %b %d %H:%M:%S %z %Y')

            if not row.relevance:
                result["relevance"] = -1
            else:
                result["relevance"] = row.relevance

            # sentiment score
            result["sentiment_score"] = get_nltk_sentiment(row.text)

            update_tags(result)

            f.write(json.dumps(result, cls=MongoEncoder) + '\n')

def main(args):
    #load data
    tweets = []

    for tweet in tweets:
        # Extra attributes
        #Replace old 'constituent' field by new 'constituent_id', 'constituent_name'
        if "constituent" in tweet.keys():
            constituent_id, constituent_name = get_constituent_id_name(row.constituent)
            tweet['constituent_id'] = constituent_id
            tweet['constituent_name'] = constituent_name

        # created at - date
        if isinstance(tweet["created_at"], str):
            tweet['date'] = datetime.strptime(tweet["created_at"], '%a %b %d %H:%M:%S %z %Y')

        if "relevance" not in tweet.keys():
            tweet["relevance"] = -1

        # sentiment score
        tweet["sentiment_score"] = get_nltk_sentiment(tweet["text"])

        #TO DO
        tweet = update_tags(tweet)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage, MongoEncoder
    from utils.twitter_analytics_helpers import *
    main(args)