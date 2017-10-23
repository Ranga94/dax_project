import string
from google.cloud import translate
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from nltk.tag import StanfordNERTagger
import os

def main(arguments):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = arguments.google_key_path

    #start tagger


def analytics(tweets_to_check, collection, language, searchQuery, constituent, st):
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

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    parser.add_argument('tagger_dir_1', help='Tagging server directory')
    parser.add_argument('tagger_dir_2', help='Tagging server directory')
    args = parser.parse_args()
    main(args)

operations = []

# for doc in collection.find({"date": {"$exists":False}}):
for doc in all_tweets:
    date = datetime.strptime(doc['created_at'], '%a %b %d %H:%M:%S %z %Y')

    operations.append(
        UpdateOne({"_id": doc["_id"]}, {"$set": {"date": date}})
    )

    # Send once every 1000 in batch
    if (len(operations) == 1000):
        print("Performing bulk write")
        collection.bulk_write(operations, ordered=False)
        operations = []

if (len(operations) > 0):
    collection.bulk_write(operations, ordered=False)