import sys
sys.path.insert(0, '../utils')
from DB import DB
import tweepy
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import string
from google.cloud import translate
import time
from datetime import datetime


#current_constituent = ['BMW','adidas', 'Deutsche Bank', 'EON', 'Commerzbank']
current_constituent = ['Deutsche Bank', 'EON', 'Commerzbank']


all_constituents = ['Allianz', 'adidas',
                    'BASF', 'Bayer', 'Beiersdorf',
                    'BMW', 'Commerzbank','Continental',
                    'Daimler',
                    'Deutsche Bank', 'Deutsche Börse',
                    'Deutsche Post', 'Deutsche Telekom', 'EON',
                    'Fresenius Medical Care', 'Fresenius', 'HeidelbergCement',
                    'Henkel vz', 'Infineon', 'Linde',
                    'Lufthansa', 'Merck',
                    'Münchener Rückversicherungs-Gesellschaft', 'ProSiebenSat1 Media',
                    'RWE', 'SAP', 'Siemens',
                    'thyssenkrupp', 'Volkswagen (VW) vz','Vonovia']


''''
args:
0: connection string
1: database
2: collection
3: language, 'en' or 'de'

tweets in english will have:
['text']: original text
['semi_processed_text']: tokenization, link removal, joined into a single string
['processed_text']: tokenization, stop word removal, link removal, stemming


tweets in german will have:
['text']: origial text
['semi_processed_text_de']: tokenization, link removal, joined again
['semi_processed_text']: translation of semi_processed_text_de
['processed text']: tokenization, stop word removal, stemming

'''


def get_tweets(argv):
    API_KEY = "fAFENmxds3YFgUqHt974ZGsov"
    API_SECRET = 'zk8IRc6WQPZ8dc2yGh8gJClEMDlL6I3L4DYIC4ZkoHvjIw4QgN'

    api = load_api(API_KEY, API_SECRET)

    # this is what we're searching for
    maxTweets = 10000000  # Some arbitrary large number
    #maxTweets = 10
    tweetsPerQry = 100  # this is the max the API permits
    #tweetsPerQry = 10
    language = argv[3]

    database = DB(argv[0], argv[1])

    constituents_collection = database.get_collection('constituents')

    logging_collection = database.get_collection('tweet_logs')

    logging_collection.find_one_and_update({'date':time.strftime("%d/%m/%Y")}, {'$set': {'date': time.strftime("%d/%m/%Y")}}, upsert=True)

    for constituent in current_constituent:
        #get constituent data for the search query
        constituent_data = constituents_collection.find_one({'name':constituent})
        keywords = constituent_data['keywords']
        exclusions = constituent_data['exclusions']
        filter = constituent_data['filter']
        filter_set = set(filter)

        k = " OR ".join(keywords)
        ex = " ".join(exclusions)
        searchQuery = k + " " + ex

        sinceId = None
        max_id = -1
        tweetCount = 0


        print("Downloading max {0} tweets for {1} in {2}".format(maxTweets, constituent, language))
        while tweetCount < maxTweets:
            list_of_tweets = []
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

                if language != "en":
                    if not do_translation(new_tweets):
                        if not do_translation(new_tweets):
                            continue


                for tweet in new_tweets:
                    #add language and date
                    document = tweet._json

                    if language == 'en':
                        document.update(preprocess_tweet(document['text']))
                    else:
                        document.update(preprocess_tweet(document['text_en']))

                    text_set = set(document['semi_processed_text'].split(' '))
                    if len(filter_set.intersection(text_set)) == 0:
                        continue

                    document['search_term'] = searchQuery
                    document['constituent'] = constituent
                    document['language'] = language

                    date = datetime.strptime(document['created_at'], '%a %b %d %H:%M:%S %z %Y')
                    document['date'] = date

                    if 'retweeted_status' in document:
                        document.pop('retweeted_status', None)


                    list_of_tweets.append(document)
                    #print(document['processed_text'])

                #Logging
                result = None

                if list_of_tweets:
                    result = database.insert_many(argv[2], list_of_tweets)

                if result is not None:
                    print("Inserted {} tweets".format(len(result.inserted_ids)))
                    logging_collection.find_one_and_update({'date': time.strftime("%d/%m/%Y")},
                                                           {'$inc': {'inserted_tweets.{}.{}'.format(language,constituent): len(result.inserted_ids)}}, upsert=True)


                tweetCount += len(new_tweets)
                print("Downloaded {0} tweets".format(tweetCount))
                max_id = new_tweets[-1].id
            except tweepy.TweepError as e:
                # Just exit if any error
                print("some error : " + str(e))
                break

    return "Downloaded tweets"


def load_api(API_KEY, API_SECRET):
    auth = tweepy.AppAuthHandler(API_KEY, API_SECRET)

    api = tweepy.API(auth, wait_on_rate_limit=True,
                     wait_on_rate_limit_notify=True)

    if (not api):
        print("Can't Authenticate")
        sys.exit(-1)
    else:
        return api

def preprocess_tweet(text:str):
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

def do_translation(to_translate:list):
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

        return True
    else:
        return False


if __name__ == "__main__":
    get_tweets(sys.argv[1:])