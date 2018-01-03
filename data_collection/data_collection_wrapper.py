import sys
from datetime import datetime
from copy import deepcopy

def main(args):
    print("Collection logs: {}".format(str(datetime.now())))
    try:
        print("Executing news_collection_orbis")
        #news_collection_orbis.main(args)
    except Exception as e:
        print(e)
    try:
        print("Executing historical_scraper")
        args_1 = deepcopy(args)
        args_1.all = True
        #historical_scraper.main(args_1)
    except Exception as e:
        print(e)
    try:
        print("Executing news_collection_zephyr")
        #news_collection_zephyr.main(args)
    except Exception as e:
        print(e)
    try:
        print("Executing yahoo_finance")
        #yahoo_finance.main(args)
    except Exception as e:
        print(e)
    try:
        print("Executing reuters_scraper")
        #reuters_scraper.main(args)
    except Exception as e:
        print(e)
    try:
        print("Executing stocktwits")
        #stocktwits.main(args)
    except Exception as e:
        print(e)
    try:
        print("Executing bloomberg_scraper")
        #bloomberg_scraper.main(args)
    except Exception as e:
        print(e)
    try:
        print("Executing ticker_data")
        #ticker_data.main(args)
    except Exception as e:
        print(e)
    try:
        print("Executing analyst opinions web scraping")
        args_2 = deepcopy(args)
        args_2.sql_connection_string = args.param_connection_string
        args_2.service_key_path = args.google_key_path
        args_2.display_parameter = 1
        if args.environment == 'test':
            args_2.table_storage = 'pecten_dataset_{}.{}'.format(args.environment, args.analyst_opinions_table)
        else:
            args_2.table_storage = 'pecten_dataset.{}'.format(args.analyst_opinions_table)
        webscraping_analyst_BQ.main(args)
    except Exception as e:
        print(e)
    try:
        print("Executing twitter_collection")
        #twitter_collection.main(args)
    except Exception as e:
        print(e)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The connection string')
    parser.add_argument('environment', help='production or test')
    parser.add_argument('analyst_opinions_table')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from data_collection.twitter import twitter_collection as twitter_collection
    from data_collection.financial import historical_scraper as historical_scraper
    from data_collection.financial import ticker_data as ticker_data
    from data_collection.news import bloomberg_scraper as bloomberg_scraper
    from data_collection.news import news_collection_orbis as news_collection_orbis
    from data_collection.news import news_collection_zephyr as news_collection_zephyr
    from data_collection.news import reuters_scraper as reuters_scraper
    from data_collection.rss_feeds import yahoo_finance as yahoo_finance
    from data_collection.stocktwits_collection import stocktwits as stocktwits
    from data_collection.analyst_rating_webscraping import webscraping_analyst_BQ as webscraping_analyst_BQ
    main(args)