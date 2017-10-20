from sqlalchemy import *
from pymongo import MongoClient, errors
import tweepy
import string
from google.cloud import translate
from datetime import datetime
import time
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from nltk.tag import StanfordNERTagger
import os
import smtplib
import sys
import jsonpickle
from bson.son import SON

def main(arguments):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = arguments.google_key_path

    parameters = list(get_parameters(arguments.param_connection_string))

    languages = parameters[0].split(',')
    parameters.append(arguments.param_connection_string)

    for lang in languages:
        parameters[0] = lang
        get_tweets(*parameters)

    send_mail(parameters[3], arguments.param_connection_string)

def get_tweets(language, tweetsPerQry, maxTweets, data_connection_string, database, collection, logging_flag, param_connection_string):
    api = load_api(param_connection_string)

    #For now pass data_connection_string, but in reality it should be param_connection_string
    all_constituents = get_constituents(data_connection_string)

    if language != "en":
        tweetsPerQry = 7

    for constituent_id, constituent_name in all_constituents:
        #For now, pass data_connection string. Later change it to param_connection_string
        searchQuery = get_search_string(constituent_id, data_connection_string)

        sinceId = get_max_id(data_connection_string,database,collection)
        max_id = -1
        tweetCount = 0

        print("Downloading max {0} tweets for {1} in {2}".format(maxTweets, constituent_name, language))
        while tweetCount < maxTweets:
            try:
                if (max_id <= 0):
                    if (not sinceId):
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry, lang=language)
                    else:
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                since_id=sinceId, lang=language)
                else:
                    if (not sinceId):
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                max_id=str(max_id - 1), lang=language)
                    else:
                        new_tweets = api.search(q=searchQuery, count=tweetsPerQry,
                                                max_id=str(max_id - 1),
                                                since_id=sinceId, lang=language)
                if not new_tweets:
                    print("No more tweets found")
                    break

                tweetCount += len(new_tweets)
                print("Downloaded {0} tweets".format(tweetCount))
                max_id = max(sinceId, max_id, new_tweets[-1].id)

                #save tweets
                save_tweets(constituent_name, new_tweets, data_connection_string, database)

            except tweepy.TweepError as e:
                # Just exit if any error
                print("some error : " + str(e))
                break

        #logging
        if logging_flag:
            logging(constituent_name, constituent_id, tweetCount, language, data_connection_string, database)

    return "Downloaded tweets"

def save_tweets(constituent_name, tweets, connection_string, database):
    client = MongoClient(connection_string)
    db = client[database]
    collection = db["tweets_test"]

    try:
        result = collection.insert_many(tweets, ordered=False)
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

def update_tweets(tweets_to_check, collection, language, searchQuery, constituent, st):
    sia = SIA()

    tokenizer = TweetTokenizer(preserve_case=True, reduce_len=True, strip_handles=False)

    list_of_tweets = []
    for tweet in tweets_to_check:
        doc = tweet._json

        if collection.find({"id_str": doc["id_str"]}, {"id_str": 1}).limit(1):
            continue

        list_of_tweets.append(doc)

    if language != "en":
        if not do_translation(list_of_tweets):
            if not do_translation(list_of_tweets):
                return None


    for document in list_of_tweets:
        if language == 'en':
            document.update(preprocess_tweet(document['text']))
        else:
            document.update(preprocess_tweet(document['text_en']))

        document['search_term'] = searchQuery
        document['constituent'] = constituent
        document['language'] = language

        date = datetime.strptime(document['created_at'], '%a %b %d %H:%M:%S %z %Y')
        document['date'] = date

        #Update sentiment
        sentiment_score,sentiment = get_nltk_sentiment(document["semi_processed_text"], sia)
        document["nltk_sentiment_score"] = sentiment
        document["nltk_sentiment_number"] = sentiment_score

        #Update tags
        document["tag_LOCATION"] = list()
        document["tag_PERSON"] = list()
        document["tag_ORGANIZATION"] = list()
        document["tag_MONEY"] = list()
        document["tag_PERCENT"] = list()
        document["tag_DATE"] = list()
        document["tag_TIME"] = list()

        if language == 'en':
            text = document['text']
        else:
            text = document['text_en']

        for word, tag in get_tags(text, st, tokenizer):
            if tag != "O":
                document["tag_" + tag].append(word)

        if 'retweeted_status' in document:
            document.pop('retweeted_status', None)

    return list_of_tweets

def get_nltk_sentiment(text, sia):

    res = sia.polarity_scores(text)

    if res["compound"] < -0.25:
        return res["compound"],"Negative"
    elif res["compound"] > 0.25:
        return res["compound"],"Positive"
    else:
        return res["compound"],"Neutral"

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

def do_translation(to_translate):
    translate_client = None
    try:
        translate_client = translate.Client()
    except Exception as e:
        #print("Error translating. Skipping...")
        print(e)
        return False


    texts = []
    tokenizer = TweetTokenizer(preserve_case=False, reduce_len=True, strip_handles=False)

    for tweet in to_translate:
        tokens = tokenizer.tokenize(tweet._json['text'])
        # remove links
        no_url_tokens = [word for word in tokens if 'http' not in word]
        texts.append(" ".join(no_url_tokens))


    texts = [tweet._json['text'] for tweet in to_translate]

    try:
        translations = translate_client.translate(texts, target_language='en')
    except Exception as e:
        #print("Error translating. Skipping...")
        print(e)
        return False

    if len(translations) == len(to_translate):
        for i in range(0, len(translations)):
            to_translate[i]._json['text_en'] = translations[i]['translatedText']

        time.sleep(1)
        return True
    else:
        return False

def preprocess_tweet(text):
    # Tokenize the tweet text
    tokenizer = TweetTokenizer(preserve_case=False, reduce_len=True, strip_handles=False)
    tokens = tokenizer.tokenize(text)

    # remove links
    no_url_tokens = [word for word in tokens if 'http' not in word]

    no_url_joined = " ".join(no_url_tokens)

    # remove stop words and punctuation
    stop_words = set(stopwords.words('english'))
    punct = string.punctuation
    punct_1 = punct.replace('#', '')
    punct_2 = punct_1.replace('@', '')
    stop_words.update(punct_2)
    stop_words.add('...')

    filtered_tokens = [word for word in no_url_tokens if not word in stop_words]

    stemmer = PorterStemmer()
    stemmed_tokens = [stemmer.stem(word) if (word[0] != '#' and word[0] != '@') else word for word in filtered_tokens]

    return {'semi_processed_text': no_url_joined, 'processed_text': stemmed_tokens}

def get_tags(text, st, tokenizer):
    new_text = text.replace('€','$')
    tokenized_text = tokenizer.tokenize(new_text)
    classified_text = st.tag(tokenized_text)
    return classified_text

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
    server.quit()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    args = parser.parse_args()
    main(args)