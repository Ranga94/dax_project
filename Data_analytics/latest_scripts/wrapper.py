import sys

def main(args):
    try:
        print("country_data")
        get_country_data(args)
    except Exception as e:
        print(e)
    try:
        print("get_influencer_price_tweets")
        get_influencer_price_tweets(args)
    except Exception as e:
        print(e)
    try:
        print("get_influencer_prices")
        get_influencer_prices(args)
    except Exception as e:
        print(e)
    try:
        print("get_news_all")
        get_news_all(args)
    except Exception as e:
        print(e)
    try:
        print("get_news_analytics_daily_sentiment_bq")
        get_news_analytics_daily_sentiment_bq(args)
    except Exception as e:
        print(e)
    try:
        print("get_news_analytics_topic_articles")
        get_news_analytics_topic_articles(args)
    except Exception as e:
        print(e)
    try:
        print("get_news_analytics_topic_sentiment_bq")
        get_news_analytics_topic_sentiment_bq(args)
    except Exception as e:
        print(e)
    try:
        print("get_news_tags_bq")
        get_news_tags_bq(args)
    except Exception as e:
        print(e)
    try:
        print("get_target_prices")
        get_target_prices(args)
    except Exception as e:
        print(e)
    try:
        print("get_twitter_analytics_latest_price_tweets")
        get_twitter_analytics_latest_price_tweets(args)
    except Exception as e:
        print(e)
    try:
        print("get_twitter_analytics_latest_price_tweets")
        get_twitter_sentiment_count_daily(args)
    except Exception as e:
        print(e)
    try:
        print("get_twitter_sentiment_popularity")
        get_twitter_sentiment_popularity(args)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    parser.add_argument('environment', help='production or test')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from Data_analytics.latest_scripts.country_data import get_country_data
    from Data_analytics.latest_scripts.influencer_prices import get_target_prices as get_influencer_prices
    from Data_analytics.latest_scripts.influencer_price_tweets import get_influencer_price_tweets
    from Data_analytics.latest_scripts.news_all import get_news_all
    from Data_analytics.latest_scripts.news_analytics_topic_articles import get_news_analytics_topic_articles
    from Data_analytics.latest_scripts.news_analytics_daily_sentiment import get_news_analytics_daily_sentiment_bq
    from Data_analytics.latest_scripts.news_anlytics_topic_sentiment import get_news_analytics_topic_sentiment_bq
    from Data_analytics.latest_scripts.news_tags import get_news_tags_bq
    from Data_analytics.latest_scripts.target_prices import get_target_prices
    from Data_analytics.latest_scripts.twitter_analytics_latest_price_tweets import get_twitter_analytics_latest_price_tweets
    from Data_analytics.latest_scripts.twitter_sentiment_count_daily import get_twitter_sentiment_count_daily
    from Data_analytics.latest_scripts.twitter_sentiment_popularity import get_twitter_sentiment_popularity
    main(args)
