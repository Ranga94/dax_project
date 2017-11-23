import sys
import os
from google.cloud import bigquery
from datetime import datetime
import json
import time

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

def update_from_bigquery(args):
    #load data
    storage = Storage(args.google_key_path)
    tagger = TU()

    columns = ["CONSTITUENT_ID"]
    table = "MASTER_CONSTITUENTS"

    constituents = storage.get_sql_data(sql_connection_string=args.param_connection_string,
                                        sql_table_name=table,
                                        sql_column_list=columns)

    start_time = time.time()

    for item in constituents:
        constituent_id = item[0]
        print("Loading {}".format(constituent_id))
        while True:

            try:
                # get max id inserted from target table
                q = "SELECT max(id) as max_id " \
                    "FROM `pecten_dataset.tweets` " \
                    "WHERE constituent_id = '{}'".format(constituent_id)

                max_id = storage.get_bigquery_data(q, iterator_flag=False)[0]["max_id"]

                if not max_id:
                    query = "SELECT text, id,favorite_count, source, retweeted,entities," \
                            "id_str,retweet_count,favorited,user,lang,created_at,place," \
                            "constituent_name,constituent_id,search_term, relevance " \
                            "FROM `pecten_dataset.tweets_unmodified` " \
                            "WHERE constituent_id = '{}' " \
                            "ORDER BY id ASC".format(constituent_id)

                else:
                    query = "SELECT text, id,favorite_count, source, retweeted,entities," \
                            "id_str,retweet_count,favorited,user,lang,created_at,place," \
                            "constituent_name,constituent_id,search_term, relevance " \
                            "FROM `pecten_dataset.tweets_unmodified` " \
                            "WHERE id > {} AND constituent_id = '{}' " \
                            "ORDER BY id ASC".format(max_id, constituent_id)

                tweets = storage.get_bigquery_data(query)

                if tweet.total_rows == 0:
                    print("Finished for {}".format(constituent_id))
                    break

                operations = []
                records = 0

                for tweet in tweets:
                    row = {}
                    row["text"] = tweet["text"]
                    row["id"] = tweet["id"]
                    row["favorite_count"] = tweet["favorite_count"]
                    row["source"] = tweet["source"]
                    row["retweeted"] = tweet["retweeted"]
                    row["entities"] = tweet["entities"]
                    row["id_str"] = tweet["id_str"]
                    row["retweet_count"] = tweet["retweet_count"]
                    row["favorited"] = tweet["favorited"]
                    row["user"] = tweet["user"]
                    row["lang"] = tweet["lang"]
                    row["created_at"] = tweet["created_at"]
                    row["place"] = tweet["place"]
                    row["constituent_name"] = tweet["constituent_name"]
                    row["constituent_id"] = tweet["constituent_id"]
                    row["search_term"] = tweet["search_term"]
                    row["relevance"] = tweet["relevance"]

                    # Additional fields
                    if isinstance(tweet["created_at"], str):
                        row['date'] = convert_timestamp(tweet["created_at"])

                    # sentiment score
                    row["sentiment_score"] = get_nltk_sentiment(tweet["text"])

                    # TO DO
                    tagged_text = tagger.get_spacy_entities(tweet["text"])
                    row["entity_tags"] = get_spacey_tags(tagged_text)

                    operations.append(row)
                    # print(row)

                    if len(operations) == 1000:
                        result = storage.insert_bigquery_data('pecten_dataset', 'tweets', operations)
                        records += 1000
                        print("Performed bulk write of {} records".format(records))
                        if not result:
                            print("Records not inserted")

                        operations = []

                if len(operations) > 0:
                    result = storage.insert_bigquery_data('pecten_dataset', 'tweets', operations)
                    records += 1000
                    if not result:
                        print("Records not inserted")

            except Exception as e:
                print(e)

    print("--- %s seconds ---" % (time.time() - start_time))

def update_from_bigquery_file(args):
    # load data
    storage = Storage(args.google_key_path)
    tagger = TU()

    query = "SELECT text, id,favorite_count, source, retweeted,entities," \
            "id_str,retweet_count,favorited,user,lang,created_at,place," \
            "constituent_name,constituent_id,search_term, relevance " \
            "FROM `pecten_dataset.tweets_unmodified`"

    tweets = storage.get_bigquery_data(query, iterator_flag=False)
    file_name = "tweets.json"

    start_time = time.time()
    operations = []
    records = 0
    total = 0

    print("Writing local file")
    with open(file_name, "w") as f:
        for tweet in tweets:
            row = {}
            row["text"] = tweet["text"]
            row["id"] = tweet["id"]
            row["favorite_count"] = tweet["favorite_count"]
            row["source"] = tweet["source"]
            row["retweeted"] = tweet["retweeted"]
            row["entities"] = tweet["entities"]
            row["id_str"] = tweet["id_str"]
            row["retweet_count"] = tweet["retweet_count"]
            row["favorited"] = tweet["favorited"]
            row["user"] = tweet["user"]
            row["lang"] = tweet["lang"]
            row["created_at"] = tweet["created_at"]
            row["place"] = tweet["place"]
            row["constituent_name"] = tweet["constituent_name"]
            row["constituent_id"] = tweet["constituent_id"]
            row["search_term"] = tweet["search_term"]
            row["relevance"] = tweet["relevance"]

            # Additional fields
            if isinstance(tweet["created_at"], str):
                row['date'] = convert_timestamp(tweet["created_at"])

            # sentiment score
            row["sentiment_score"] = get_nltk_sentiment(tweet["text"])

            # TO DO
            tagged_text = tagger.get_spacy_entities(tweet["text"])
            row["entity_tags"] = get_spacey_tags(tagged_text)

            f.write(json.dumps(row, cls=MongoEncoder) + '\n')
            records += 1
            total += 1

            if records == 2000:
                print("Written {} records".format(total))
                records = 0

    print("--- %s seconds ---" % (time.time() - start_time))
    print("Processed {} records".format(records))

def update_from_bigquery_split(args):
    #load data
    storage = Storage(args.google_key_path)
    tagger = TU()

    columns = ["CONSTITUENT_ID"]
    table = "MASTER_CONSTITUENTS"


    constituents = storage.get_sql_data(sql_connection_string=args.param_connection_string,
                                        sql_table_name=table,
                                        sql_column_list=columns)

    #constituents = [("BMWDE8170003036", )]

    start_time = time.time()

    for item in constituents:
        constituent_id = item[0]
        print("Loading {}".format(constituent_id))

        try:

            query = "SELECT text, id,favorite_count, source, retweeted,entities," \
                    "id_str,retweet_count,favorited,user,lang,created_at,place," \
                    "constituent_name,constituent_id,search_term, relevance " \
                    "FROM `pecten_dataset.tweets_unmodified` " \
                    "WHERE constituent_id = '{}' ".format(constituent_id)

            tweets = storage.get_bigquery_data(query)

            operations = []
            records = 0

            for tweet in tweets:
                row = {}
                row["text"] = tweet["text"]
                row["id"] = tweet["id"]
                row["favorite_count"] = tweet["favorite_count"]
                row["source"] = tweet["source"]
                row["retweeted"] = tweet["retweeted"]
                row["entities"] = tweet["entities"]
                row["id_str"] = tweet["id_str"]
                row["retweet_count"] = tweet["retweet_count"]
                row["favorited"] = tweet["favorited"]
                row["user"] = tweet["user"]
                row["lang"] = tweet["lang"]
                row["created_at"] = tweet["created_at"]
                row["place"] = tweet["place"]
                row["constituent_name"] = tweet["constituent_name"]
                row["constituent_id"] = tweet["constituent_id"]
                row["search_term"] = tweet["search_term"]
                row["relevance"] = tweet["relevance"]

                # Additional fields
                if isinstance(tweet["created_at"], str):
                    date = tweet["created_at"]
                    ts = time.strptime(date, "%a %b %d %H:%M:%S %z %Y")
                    ts = time.strftime('%Y-%m-%d %H:%M:%S', ts)
                    row['date'] = ts

                # sentiment score
                row["sentiment_score"] = get_nltk_sentiment(tweet["text"])

                # TO DO
                tagged_text = tagger.get_spacy_entities(tweet["text"])
                row["entity_tags"] = get_spacey_tags(tagged_text)

                operations.append(row)
                # print(row)

                if len(operations) == 1000:
                    result = None
                    result = storage.insert_bigquery_data('pecten_dataset', 'tweets', operations)
                    records += 1000
                    print("Performed bulk write of {} records".format(records))
                    if not result:
                        print("Records not inserted")

                    operations = []

            if len(operations) > 0:
                result = None
                result = storage.insert_bigquery_data('pecten_dataset', 'tweets', operations)
                records += 1000
                if not result:
                    print("Records not inserted")

        except Exception as e:
            print(e)

    print("--- %s seconds ---" % (time.time() - start_time))

class TwitterStaticAnalytics:
    def __init__(self):
        self.tagger = TaggingUtils()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage, MongoEncoder
    from utils.TaggingUtils import TaggingUtils as TU
    from utils.twitter_analytics_helpers import *
    #main(args)
    update_from_bigquery_split(args)