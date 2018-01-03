from bs4 import BeautifulSoup
import re, requests
from fake_useragent import UserAgent
from pprint import pprint
import sys
from datetime import datetime

# Parses Bloomberg article and returns dictionary with data
def get_article(link, ua):
    # Make a request using random user agent and create bs4 object from <main> tag.
    soup = BeautifulSoup(requests.get(link).content, 'html.parser').find('main')

    # Check if title is there if not it's not correctly formated article
    try:
        title = soup.find('h1').text.strip()
    except AttributeError:
        return None

    # Get main body of text
    try:
        news = soup.find('section', {'class': 'main-column'}).find('div', {'class': 'body-copy'})
    except AttributeError:
        return None

    # Remove javascript code from the text
    news_text = re.sub("\n{(.*?)\"}\n", "", news.text)

    # Get date of publishing the article
    published = soup.find('time', {'itemprop': 'datePublished'}).text
    published = re.sub("{(.*?)\"}", "", published).strip()

    # Get source if it's present in the article
    try:
        source = news.find('em').text
    except AttributeError:
        source = None

    published = published.split(" ")
    published = published[:-1]
    published = [w.replace(",", "") for w in published]
    if len(published[1]) == 0:
        published[1] = "0" + published[1]
    published = " ".join(published)
    published = datetime.strptime(published, '%B %d %Y %I:%M %p')
    published = published.strftime('%Y-%m-%d %H:%M:%S')

    doc = {'news_title': title, 'news_article_txt': news_text, 'news_date': published, 'news_origin': "Bloomberg",
            'news_source':source}
    return doc

def get_bloomberg_news(args):
    from utils.Storage import Storage
    from utils import twitter_analytics_helpers as tah
    from utils.TaggingUtils import TaggingUtils as TU
    from utils import logging_utils as logging_utils

    tagger = TU()
    storage_client = Storage(google_key_path=args.google_key_path)

    #Get parameters
    param_table = "PARAM_NEWS_COLLECTION"
    parameters_list = ["LOGGING","DESTINATION_TABLE","LOGGING_TABLE"]
    where = lambda x: x["SOURCE"] == 'Bloomberg'

    parameters = tah.get_parameters(args.param_connection_string, param_table, parameters_list, where)

    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = tah.get_parameters(args.param_connection_string, common_table, common_list, common_where)

    #get constituents
    all_constituents = storage_client.get_sql_data(sql_connection_string =args.param_connection_string,
                                                   sql_table_name="PARAM_NEWS_BLOOMBERG_KEYS",
                                                   sql_column_list=["CONSTITUENT_ID",
                                                                    "CONSTITUENT_NAME",
                                                                    "URL_KEY",
                                                                    "PAGES"])[:2]

    # Define random user agent object
    #ua = UserAgent()

    # Base url for search queries
    base = 'https://bloomberg.com/search?query={}&sort=time:desc&page={}'

    # Iterate over companies
    for constituent_id, constituent_name, url_key,pages in all_constituents:
        # Get date of latest news article for this constituent for Bloomberg
        query = """
        SELECT max(news_date) as last_date FROM `{}.{}`
        WHERE constituent_id = '{}' AND news_origin = 'Bloomberg'
        """.format(common_parameters["BQ_DATASET"],parameters["DESTINATION_TABLE"],constituent_id)

        try:
            result = storage_client.get_bigquery_data(query=query, iterator_flag=False)
            last_date_bq = result[0]["last_date"]
        except Exception as e:
            last_date_bq = None

        print('Scraping for {} up to {}.'.format(constituent_name, str(last_date_bq)))
        # iterate through specified page range and save articles to database
        in_target = True
        i = 1
        while in_target:
            print('Scraping for page', i)
            # format url with company name and current page number and create bs4 object from the response
            url = base.format(url_key.replace('.', '_').replace(' ', '_'), i)
            i += 1
            main_soup = BeautifulSoup(requests.get(url).content, 'html.parser')

            # check if there are any results on the page and finish the loop if none
            try:
                if 'No results for' in main_soup.find('div', {'class': 'content-no-results'}).text:
                    print('No more articles...')
                    break
            except AttributeError:
                pass

            # iterate through article elelements on the page
            results = main_soup.findAll('div', {'class': 'search-result'})
            times = [x.find('time', {'class': 'published-at'}).text.strip() for x in results]
            times = [datetime.strptime(x, '%b %d, %Y') for x in times]
            for time in times:
                if last_date_bq:
                    if time < datetime.strptime(last_date_bq.strftime("%Y-%m-%d"), "%Y-%m-%d"):
                        print('Target date reached')
                        in_target = False

            for art in results:
                to_insert = []

                art_type = art.find('article')['class']
                # check if the result is an article
                if 'type-article' in art_type:
                    url = art.find('h1').find('a')['href']
                    d = get_article(url, None)

                    if d is None:
                        continue

                    if last_date_bq:
                        if datetime.strptime(d["news_date"], "%Y-%m-%d %H:%M:%S") < \
                                datetime.strptime(last_date_bq.strftime("%Y-%m-%d"), "%Y-%m-%d"):
                            continue

                    #set extra fields:
                    #score
                    if d["news_article_txt"]:
                        d['score'] = tah.get_nltk_sentiment(str(d["news_article_txt"]))

                    # sentiment
                    d['sentiment'] = tah.get_sentiment_word(d["score"])

                    # constituent fields
                    d["constituent_id"] = constituent_id
                    d["constituent_name"] = constituent_name
                    d["constituent"] = tah.get_old_constituent_name(constituent_id)

                    #url
                    d["url"] = url

                    #show
                    d["show"] = True

                    # entity_tags
                    d["entity_tags"] = tah.get_spacey_tags(tagger.get_spacy_entities(str(d["news_title"])))

                    to_insert.append(d)


            if to_insert:
                print("Inserting records to BQ")
                try:
                    storage_client.insert_bigquery_data(common_parameters["BQ_DATASET"], parameters["DESTINATION_TABLE"], to_insert)
                except Exception as e:
                    print(e)

                if parameters["LOGGING"]:
                    doc = [{"date": datetime.now().date().strftime('%Y-%m-%d %H:%M:%S'),
                            "constituent_name": constituent_name,
                            "constituent_id": constituent_id,
                            "downloaded_news": len(to_insert),
                            "source": "Bloomberg"}]
                    logging_utils.logging(doc,common_parameters["BQ_DATASET"],parameters["LOGGING_TABLE"],storage_client)

def main(args):
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils import twitter_analytics_helpers as tah
    from utils.TaggingUtils import TaggingUtils as TU
    from utils import email_tools as email_tools

    # Get parameters
    param_table = "PARAM_NEWS_COLLECTION"
    parameters_list = ["LOGGING", "DESTINATION_TABLE", "LOGGING_TABLE"]
    where = lambda x: x["SOURCE"] == 'Bloomberg'
    parameters = tah.get_parameters(args.param_connection_string, param_table, parameters_list, where)

    # Get dataset name
    common_table = "PARAM_READ_DATE"
    common_list = ["BQ_DATASET"]
    common_where = lambda x: (x["ENVIRONMENT"] == args.environment) & (x["STATUS"] == 'active')

    common_parameters = tah.get_parameters(args.param_connection_string, common_table, common_list, common_where)
    get_bloomberg_news(args)

    q1 = """
                    SELECT a.constituent_name, a.downloaded_news, a.date, a.source
                    FROM
                    (SELECT constituent_name, SUM(downloaded_news) as downloaded_news, DATE(date) as date, source
                    FROM `{0}.{1}`
                    WHERE source = 'Bloomberg'
                    GROUP BY constituent_name, date, source
                    ) a,
                    (SELECT constituent_name, MAX(DATE(date)) as date
                    FROM `{0}.{1}`
                    WHERE source = 'Bloomberg'
                    GROUP BY constituent_name
                    ) b
                    WHERE a.constituent_name = b.constituent_name AND a.date = b.date
                    GROUP BY a.constituent_name, a.downloaded_news, a.date, a.source;
            """.format(common_parameters["BQ_DATASET"],parameters["LOGGING_TABLE"])

    q2 = """
                    SELECT constituent_name,count(*)
                    FROM `{}.{}`
                    WHERE news_origin = "Bloomberg"
                    GROUP BY constituent_name
    """.format(common_parameters["BQ_DATASET"],parameters["DESTINATION_TABLE"])


    email_tools.send_mail(args.param_connection_string,args.google_key_path,"Bloomberg",
              "PARAM_NEWS_COLLECTION",lambda x: x["SOURCE"] == "Bloomberg",q1,q2)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The MySQL connection string')
    parser.add_argument('environment', help='production or test')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils import twitter_analytics_helpers as tah
    from utils.TaggingUtils import TaggingUtils as TU
    from utils.logging_utils import *
    from utils.email_tools import *
    main(args)


