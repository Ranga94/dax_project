from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from pymongo import UpdateOne
from pymongo import MongoClient
import sys
import time
from datetime import datetime


def main(args):
    client = MongoClient(args.connection_string)
    db = client["dax_gcp"]
    collection = db["tweets"]

    sia = SIA()
    tagger = TU()

    operations = []
    records = 0

    start_time = time.time()

    for doc in collection.find({}, no_cursor_timeout=True):
        sentiment_score = get_nltk_sentiment(doc["text"],sia)

        new_values = {}
        new_values["nltk_sentiment_numeric"] = sentiment_score
        new_values["sentiment_score"] = sentiment_score

        if "relevance" not in doc:
            new_values["relevance"] = -1

        new_values['date'] = datetime.strptime(doc['created_at'], '%a %b %d %H:%M:%S %z %Y')

        # TO DO
        tagged_text = tagger.get_spacy_entities(doc["text"])
        new_values["entity_tags"] = tap.get_spacey_tags(tagged_text)

        operations.append(
            UpdateOne({"_id": doc["_id"]}, {"$set": new_values})
        )

        # Send once every 1000 in batch
        if (len(operations) == 2000):
            print("Performing bulk write")
            collection.bulk_write(operations, ordered=False)
            operations = []
            records += 2000
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
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('connection_string', help='The connection string')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.TaggingUtils import TaggingUtils as TU
    from utils import twitter_analytics_helpers as tap
    main(args)