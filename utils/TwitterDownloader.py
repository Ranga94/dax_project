import tweepy
import sys

class TwitterDownloader:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self._api = None

    def load_api(self):
        auth = tweepy.AppAuthHandler(self.api_key,self.api_secret,)

        api = tweepy.API(auth, wait_on_rate_limit=True,
                         wait_on_rate_limit_notify=True)

        if (not api):
            print("Can't Authenticate")
            sys.exit(-1)
        else:
            self._api = api

    def download(self, constituent_name, search_query, language, tweets_per_query ,since_id, max_id):
        if not self._api:
            print("API not loaded. Please execute load_api before calling this method.")
            return None

        tweet_count = 0

        try:
            if max_id <= 0:
                if not since_id:
                    new_tweets = self._api.search(q=search_query, count=tweets_per_query, lang=language)
                else:
                    new_tweets = self._api.search(q=search_query, count=tweets_per_query,
                                                  since_id=since_id, lang=language)
            else:
                if not since_id:
                    new_tweets = self._api.search(q=search_query, count=tweets_per_query,
                                                  max_id=str(max_id - 1), lang=language)
                else:
                    new_tweets = self._api.search(q=search_query, count=tweets_per_query,
                                                  max_id=str(max_id - 1),
                                                  since_id=since_id, lang=language)
            if not new_tweets:
                print("No more tweets found")

            tweet_count += len(new_tweets)
            print("Downloaded {0} tweets".format(tweet_count))
            max_id = new_tweets[-1].id
        except tweepy.TweepError as e:
            # Just exit if any error
            print("some error : " + str(e))

        return new_tweets, tweet_count, max_id

if __name__ == "__main__":
    from pprint import pprint
    import os
    from google.cloud import storage
    from pathlib import Path
    import jsonpickle
    import tempfile

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "C:\\Users\\Uly\\Desktop\\Desktop\\DAX\\dax_project\\twitter_analytics\\igenie-project-key.json"

    filename = "test.json"

    t = TwitterDownloader("fAFENmxds3YFgUqHt974ZGsov", 'zk8IRc6WQPZ8dc2yGh8gJClEMDlL6I3L4DYIC4ZkoHvjIw4QgN')
    t.load_api()
    tweets, _, _ = t.download("BMW", "BMW", "en", 10, None, -1)

    with open(filename, "w") as f:
        for item in tweets:
            f.write(jsonpickle.encode(item._json, unpicklable=False) + '\n')

    client = storage.Client()
    bucket = client.get_bucket("igenie-tweets")
    blob = bucket.blob("2017/{}".format(filename))
    blob.upload_from_filename(filename)

    os.remove(filename)









