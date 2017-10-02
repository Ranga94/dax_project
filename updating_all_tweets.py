from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from pymongo import UpdateOne
from pymongo import MongoClient
from nltk.tag import StanfordNERTagger
from nltk.tokenize import TweetTokenizer
import sys
import time
from sner import Ner

def main(argv):
    client = MongoClient("mongodb://igenie_readwrite:igenie@35.189.101.142:27017/dax_gcp")
    db = client["dax_gcp"]
    collection = db["tweets"]

    sia = SIA()
    #st = StanfordNERTagger(argv[0],argv[1],encoding='utf-8')
    tagger = Ner(host='localhost', port=9199)
    tokenizer = TweetTokenizer(preserve_case=True, reduce_len=True, strip_handles=False)

    operations = []
    cursor = collection.find({"constituent":"adidas", "nltk_sentiment_numeric":{"$exists":False}},{"_id":1,"text":1,"text_en":1,"semi_processed_text":1})
    all_tweets = list(cursor)
    records = 0

    start_time = time.time()

    for doc in all_tweets:
        sentiment_score = get_nltk_sentiment(doc["semi_processed_text"],sia)
        if "text_en" in doc:
            tags = get_tags(doc["text_en"], tagger)
        else:
            tags = get_tags(doc["text"],tagger)

        new_values = {}
        new_values["nltk_sentiment_numeric"] = sentiment_score
        new_values["tag_LOCATION"] = list()
        new_values["tag_PERSON"] = list()
        new_values["tag_ORGANIZATION"] = list()
        new_values["tag_MONEY"] = list()
        new_values["tag_PERCENT"] = list()
        new_values["tag_DATE"] = list()
        new_values["tag_TIME"] = list()

        for word, tag in tags:
            if tag != "O":
                new_values["tag_" + tag].append(word)

        #print(new_values)

        operations.append(
            UpdateOne({"_id": doc["_id"]}, {"$set": new_values})
        )

        # Send once every 1000 in batch
        if (len(operations) == 1000):
            print("Performing bulk write")
            collection.bulk_write(operations, ordered=False)
            operations = []
            records += 1000
            print("Write done. Saved {} records".format(records))

    if (len(operations) > 0):
        collection.bulk_write(operations, ordered=False)

    print("--- %s seconds ---" % (time.time() - start_time))
    print("Processed {} records".format(records))


def get_nltk_sentiment(semi_processed_text,sia):
    res = sia.polarity_scores(semi_processed_text)

    return res["compound"]

def get_tags(text, tagger):
    new_text = text.replace('€','$')
    #tokenized_text = tokenizer.tokenize(new_text)
    #classified_text = st.tag(tokenized_text)
    classified_text = tagger.get_entities(new_text)
    return classified_text


if __name__ == "__main__":
    main(sys.argv[1:])