import sys
import json
from pprint import pprint
from utils.Storage import Storage
from utils.PubsubUtils import PubsubUtils
from utils import twitter_analytics_helpers as tap
from utils.TaggingUtils import TaggingUtils as TU

def main(args):
    param_table = "PARAM_TWITTER_COLLECTION"
    parameters_list = ["CONNECTION_STRING"]

    parameters = get_parameters(args.param_connection_string, param_table, parameters_list)


    print("Getting {} tweets".format(args.script))
    if args.script == 'unmodified':
        project_name = "igenie-project"
        topic_name = "tweets-unmodified"
        sub_name = "sub-tweets-unmodified"
    else:
        project_name = "igenie-project"
        topic_name = "tweets"
        sub_name = "sub-tweets"

    mongo_connection_string = parameters["CONNECTION_STRING"]
    pull_tweets(project_name,topic_name,sub_name,mongo_connection_string,args)

def save_unmodified_tweets(message, storage_client):
    tweets = []
    encoded_data = message.data
    decoded_data = encoded_data.decode("utf-8")  # json string
    json_message = json.loads(decoded_data)  # dict
    twmessages = json_message["messages"]
    CHUNK = 50

    if twmessages:
        print("Looping tweets from pubsub message")
        for res in twmessages:
            # First get the tweet
            tweet = res["data"]

            if 'delete' in tweet:
                continue
            if 'limit' in tweet:
                continue

            tweets.append(tweet)

        # Write reamining tweets
        if len(tweets) > 0:
            # write to BigQuery and MongoDB
            if storage_client.insert_bigquery_data('pecten_dataset', 'tweets_unmodified_test', tweets):
                print("Message saved")
                message.ack()

def save_modified_tweets(message, storage_client):
    tweets = []
    encoded_data = message.data
    decoded_data = encoded_data.decode("utf-8")  # json string
    json_message = json.loads(decoded_data)  # dict
    twmessages = json_message["messages"]
    CHUNK = 50
    tagger = TU()

    if twmessages:
        print("Looping tweets from pubsub message")
        for res in twmessages:
            # First get the tweet
            tweet = res["data"]

            if 'delete' in tweet:
                continue
            if 'limit' in tweet:
                continue

            tweet['date'] = tap.convert_timestamp(tweet["created_at"])
            # sentiment score
            tweet["sentiment_score"] = tap.get_nltk_sentiment(tweet["text"])

            # TO DO
            tagged_text = tagger.get_spacy_entities(tweet["text"])
            tweet["entity_tags"] = tap.get_spacey_tags(tagged_text)
            tweet["relevance"] = -1

            tweets.append(tweet)

        # Write reamining tweets
        if len(tweets) > 0:
            # write to BigQuery and MongoDB
            if storage_client.insert_bigquery_data('pecten_dataset', 'tweets_test', tweets):
                print("Message saved to BigQuery")
                message.ack()
                if storage_client.save_to_mongodb(tweets, "dax_gcp", "tweets_test2"):
                    print("Message saved to MongoDB")

def pull_tweets(project_name,topic_name,sub_name,mongo_connection_string,args):
    if args.script == "unmodified":
       func = save_unmodified_tweets
    else:
        func = save_modified_tweets

    storage = Storage(mongo_connection_string=mongo_connection_string, google_key_path=args.google_key_path)
    ps_utils = PubsubUtils(args.google_key_path)

    print("Getting subscription {}".format(sub_name))
    if ps_utils.create_subscription(project_name,topic_name,sub_name):
        pass
    else:
        print("There was an error creating the subscription")

    print("Getting messages...")
    try:
        ps_utils.pull_messages(project_name, sub_name, func, storage)
    except Exception as e:
        print(e)

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
    parser.add_argument('script', help='modified or unmodified')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils.PubsubUtils import PubsubUtils
    from utils import twitter_analytics_helpers as tap
    from utils.TaggingUtils import TaggingUtils as TU
    main(args)