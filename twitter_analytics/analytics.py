import sys
from DB import DB
from sortedcontainers import SortedListWithKey
from collections import defaultdict

def main(argv):
    database = DB(argv[0], argv[1])


def general_analytics(cursor: list):
    vocabulary = defaultdict(int)
    top_retweeted = SortedListWithKey(key=lambda x: x["retweet_count"])
    top_fav = SortedListWithKey(key=lambda x: x["favorite_count"])
    top_followed = SortedListWithKey(key=lambda x: x["user"]['followers_count'])
    prices = []

    for tweet in cursor:
        text = tweet["processed_text"]

        for word in tweet["processed_text"]:
            vocabulary[word] += 1

        top_retweeted.add(tweet)
        if len(top_retweeted) > 50:
            top_retweeted.pop(0)

        top_fav.add(tweet)
        if len(top_fav) > 500:
            top_fav.pop(0)

        top_followed.add(tweet)
        if len(top_followed) > 50:
            top_followed.pop(0)

    return vocabulary, top_retweeted, top_fav, top_followed

def price_analytics(cursor:list):
    prices = []
    for tweet in cursor:
        text = tweet["processed_text"]

        for word in tweet["processed_text"]:
            try:
                number = float(word)
                if number < 150 and number > 50:
                    prices.append(number)
            except:
                pass

    return prices

def print_prices(prices):
    prices.sort(reverse=True)
    total = sum(prices)
    print("Average price: {}".format(total/len(prices)))
    print("Highest price: {}".format(prices[0]))
    print("Lowest price: {}".format(prices[-1]))

    prices_frequency = defaultdict(int)
    for p in prices:
        prices_frequency[p] += 1

    print("Price prediction distribution")
    for p1, p2 in sorted(prices_frequency.items(), key=lambda x: x[1], reverse=True):
        print("{}".format(p2))

def print_results(results:list):
    for item in reversed(results):
        print(item['semi_processed_text'])

def print_term_results(vocabulary:dict):
    for key,word in [('recal',"Recall"), ('sale','Sales'), ('financi', 'Financial'), ('bearish','Bearish'),
                ('bullish', 'Bullish')]:
        if key in vocabulary:
            print("{}:{}".format(word, vocabulary[key]))

def print_countries(cursor:list):
    countries = defaultdict(int)

    for tweet in cursor:
        for word in tweet["processed_text"]:
            try:
                number = float(word)
                if number < 100 and number > 50:
                    if tweet['place'] is not None:
                        if 'country_code' in tweet['place'].keys():
                            countries[tweet['place']['country_code']] += 1
                    break
            except:
                pass

    print((sorted(countries.items(), key=lambda x: x[1], reverse=True)))





if __name__ == "__main__":
    main(sys.argv[1:])