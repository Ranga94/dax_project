from pymongo import UpdateOne
from pymongo import MongoClient
import time
from sner import Ner
from pprint import pprint

LOCATIONS = ["eu", "oxford", "britain"]
locations_set = set(LOCATIONS)


def main():
    client = MongoClient("mongodb://igenie_readwrite:igenie@35.189.101.142:27017/dax_gcp")
    db = client["dax_gcp"]
    collection = db["all_news"]

    tagger = Ner(host='localhost', port=9199)

    operations = []


    records = 0
    start_time = time.time()

    for doc in collection.find({}, no_cursor_timeout=True):

        tags_temp = get_tags(doc["NEWS_TITLE_NewsDim"], tagger)
        if not tags_temp:
            continue
        tags = process_tags(tags_temp)

        new_values = {}

        new_values["tag_header_LOCATION"] = list()
        new_values["tag_header_PERSON"] = list()
        new_values["tag_header_ORGANIZATION"] = list()
        new_values["tag_header_MONEY"] = list()
        new_values["tag_header_PERCENT"] = list()
        new_values["tag_header_DATE"] = list()
        new_values["tag_header_TIME"] = list()

        for word, tag in tags:
            if tag != "O":
                new_values["tag_header_" + tag].append(word)

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


def get_nltk_sentiment(semi_processed_text, sia):
    res = sia.polarity_scores(semi_processed_text)

    return res["compound"]


def get_tags(text, tagger):
    if isinstance(text,str):
        new_text = text.replace('€', '$')
        new_text = new_text.replace('#', ' ')
        classified_text = tagger.get_entities(new_text)
        return classified_text
    else:
        return None


def get_relevance(text_clf, count_vectorizer, tf_transformer, processed_text):
    X_test_counts = count_vectorizer.transform(processed_text)
    X_test_tf = tf_transformer.transform(X_test_counts)
    predicted = text_clf.predict(X_test_tf)
    return predicted


def process_tags(tags):
    all_tags = []
    temp = []

    for word, tag in tags:
        if word.lower() in locations_set:
            tag = "LOCATION"
        if "http" in word:
            continue
        if tag == "O":
            if temp:
                all_tags.append((" ".join(temp[1]), temp[0]))
                temp = []
            else:
                continue
        else:
            if temp:
                if temp[0] == tag:
                    temp[1].append(word.lower())
                else:
                    all_tags.append((" ".join(temp[1]), temp[0]))
                    temp = [None] * 2
                    temp[0] = tag
                    temp[1] = [word.lower()]
            else:
                temp = [None] * 2
                temp[0] = tag
                temp[1] = [word.lower()]

    # pprint(all_tags)
    return all_tags

if __name__=="__main__":
    main()