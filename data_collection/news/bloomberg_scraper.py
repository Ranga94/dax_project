from bs4 import BeautifulSoup
import re, requests, pandas_datareader
from pandas_datareader import data
from time import sleep
from fake_useragent import UserAgent
from pprint import pprint
import sys
from datetime import datetime

# Parses Bloomberg article and returns dictionary with data
def get_article(link, ua):
    # Make a request using random user agent and create bs4 object from <main> tag.
    soup = BeautifulSoup(requests.get(link, headers={'User-Agent': ua.random}).content, 'html.parser').find('main')

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
    tagger = TU()
    storage_client = Storage.Storage(google_key_path=args.google_key_path)

    #Get parameters
    param_table = "PARAM_NEWS_COLLECTION"
    parameters_list = ["LOGGING"]
    where = lambda x: x["SOURCE"] == 'Bloomberg'

    parameters = get_parameters(args.param_connection_string, param_table, parameters_list, where)

    #get constituents
    all_constituents = storage_client.get_sql_data(sql_connection_string =args.param_connection_string,
                                                   sql_table_name="PARAM_NEWS_BLOOMBERG_KEYS",
                                                   sql_column_list=["CONSTITUENT_ID",
                                                                    "CONSTITUENT_NAME",
                                                                    "URL_KEY",
                                                                    "PAGES"])

    # Define random user agent object
    ua = UserAgent()

    # Base url for search queries
    base = 'https://bloomberg.com/search?query={}&sort=time:desc&page={}'

    # Iterate over companies
    for constituent_id, constituent_name, url_key,pages in all_constituents:
        # Get date of latest news article for this constituent for Bloomberg
        query = """
        SELECT max(news_date) as last_date FROM `pecten_dataset.all_news`
        WHERE constituent_id = '{}' AND news_origin = 'Bloomberg'
        """.format(constituent_id)

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
            main_soup = BeautifulSoup(requests.get(url, headers={'User-Agent': ua.random}).content, 'html.parser')

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
                    if time < last_date_bq:
                        print('Target date reached')
                        in_target = False

            for art in results:
                to_insert = []

                art_type = art.find('article')['class']
                # check if the result is an article
                if 'type-article' in art_type:
                    url = art.find('h1').find('a')['href']
                    d = get_article(url, ua)

                    if d is None:
                        continue

                    if last_date_bq:
                        if datetime.strptime(d["news_date"], "%Y-%m-%d %H:%M:%S") < last_date_bq:
                            continue

                    #set extra fields:
                    #score
                    if d["news_article_txt"]:
                        d['score'] = get_nltk_sentiment(str(d["news_article_txt"]))

                    # sentiment
                    d['sentiment'] = get_sentiment_word(d["score"])

                    # constituent fields
                    d["constituent_id"] = constituent_id
                    d["constituent_name"] = constituent_name
                    d["constituent"] = get_old_constituent_name(constituent_id)

                    #url
                    d["url"] = url

                    #show
                    d["show"] = True

                    # entity_tags
                    d["entity_tags"] = get_spacey_tags(tagger.get_spacy_entities(str(d["news_title"])))

                    to_insert.append(d)


            if to_insert:
                print("Inserting records to BQ")
                try:
                    storage_client.insert_bigquery_data('pecten_dataset', 'all_news', to_insert)
                except Exception as e:
                    print(e)

                if parameters["LOGGING"]:
                    doc = [{"date": datetime.now().date().strftime('%Y-%m-%d %H:%M:%S'),
                            "constituent_name": constituent_name,
                            "constituent_id": constituent_id,
                            "downloaded_news": len(to_insert),
                            "source": "Bloomberg"}]
                    logging(doc,'pecten_dataset',"news_logs",storage_client)



def main(args):
    get_bloomberg_news(args)

    q1 = """
                    SELECT a.constituent_name, a.downloaded_news, a.date, a.source
                    FROM
                    (SELECT constituent_name, SUM(downloaded_news) as downloaded_news, DATE(date) as date, source
                    FROM `pecten_dataset.news_logs`
                    WHERE source = 'Bloomberg'
                    GROUP BY constituent_name, date, source
                    ) a,
                    (SELECT constituent_name, MAX(DATE(date)) as date
                    FROM `igenie-project.pecten_dataset.news_logs`
                    WHERE source = 'Bloomberg'
                    GROUP BY constituent_name
                    ) b
                    WHERE a.constituent_name = b.constituent_name AND a.date = b.date
                    GROUP BY a.constituent_name, a.downloaded_news, a.date, a.source;
            """

    q2 = """
                    SELECT constituent_name,count(*)
                    FROM `pecten_dataset.all_news`
                    WHERE news_origin = "Bloomberg"
                    GROUP BY constituent_name
    """

    send_mail(args.param_connection_string,args.google_key_path,"Bloomberg",
              "PARAM_NEWS_COLLECTION",lambda x: x["SOURCE"] == "Bloomberg",q1,q2)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The MySQL connection string')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage
    from utils.twitter_analytics_helpers import *
    from utils.TaggingUtils import TaggingUtils as TU
    from utils.logging_utils import *
    from utils.email_tools import *
    main(args)


