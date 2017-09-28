from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from pymongo import UpdateOne
from pymongo import MongoClient
from nltk.tag import StanfordNERTagger
from nltk.tokenize import TweetTokenizer
import sys

def main(argv):
    client = MongoClient("mongodb://igenie_readwrite:igenie@35.189.101.142:27017/dax_gcp")
    db = client["dax_gcp"]
    collection = db["tweets"]

    sia = SIA()
    st = StanfordNERTagger(argv[0],argv[1],encoding='utf-8')
    tokenizer = TweetTokenizer(preserve_case=True, reduce_len=True, strip_handles=False)

    operations = []

    for doc in collection.find({"nltk_sentiment_numeric":{"$exists":False}},{"_id":1,"text":1,"semi_processed_text":1},no_cursor_timeout=True):
        sentiment_score = get_nltk_sentiment(doc["semi_processed_text"],sia)
        tags = get_tags(doc["text"],st,tokenizer)

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
            print("Write done.")

    if (len(operations) > 0):
        collection.bulk_write(operations, ordered=False)


def get_nltk_sentiment(semi_processed_text,sia):
    res = sia.polarity_scores(semi_processed_text)

    return res["compound"]

def get_tags(text, st, tokenizer):
    new_text = text.replace('€','$')
    tokenized_text = tokenizer.tokenize(new_text)
    classified_text = st.tag(tokenized_text)
    return classified_text


if __name__ == "__main__":
    main(sys.argv[1:])