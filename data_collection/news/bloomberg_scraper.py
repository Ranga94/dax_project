from bs4 import BeautifulSoup
import re, datetime, requests, pandas_datareader
from pandas_datareader import data
from time import sleep
from fake_useragent import UserAgent
from pprint import pprint
import sys

# Return stock information from Yahoo for a given date.
def get_stock_data(date, stock):
    # transform date string to correct format
    new_date = date.split(' ')
    month = new_date[0][:3]
    new_date = ' '.join([month] + new_date[1:]).replace(',', '').strip()
    new_date = new_date[:new_date.rfind(' ')] + new_date[new_date.rfind(' ') + 1:]

    # convert date string to datetime object
    dt = datetime.datetime.strptime(new_date, '%b %d %Y %I:%M%p')
    dt2 = dt + datetime.timedelta(days=1)

    # get stock value for the given date
    try:
        df = data.DataReader(name=stock, data_source='yahoo', start=dt, end=dt2)
    except pandas_datareader._utils.RemoteDataError:
        sleep(5)
        try:
            df = data.DataReader(name=stock, data_source='yahoo', start=dt, end=dt2)
        except pandas_datareader._utils.RemoteDataError:
            sleep(15)
            try:
                df = data.DataReader(name=stock, data_source='yahoo', start=dt, end=dt2)
            except:
                return None
    sleep(2)
    # return dataframe as dictionary
    return df.to_dict('records')[0]

# Parses Bloomberg article and returns dictionary with data
def get_article(link):
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

    return {'news_title': title, 'news_article_txt ': news_text, 'news_date ': published, 'news_source': "Bloomberg",
            'news_pubication':source}

def main(args):
    tagger = TU()
    storage_client = Storage(google_key_path=args.google_key_path)

    #Get parameters
    param_table = "PARAM_NEWS_COLLECTION"
    parameters_list = ["LOGGING"]
    where = lambda x: x["SOURCE"] == 'bloomberg'

    parameters = get_parameters(args.param_connection_string, param_table, parameters_list)

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

    '''
    # Read csv file in format: Company Name,Ticker Symbol,Number of Pages to a dictionary
    companies = {}
    with open('companies.csv', 'r') as f:
        lines = f.read().split('\n')
        for line in enumerate(lines, 1):
            cmp = line[1].split(',')
            companies[line[0]] = {'name': cmp[0].replace('.', '_').replace(' ', '_'), 'ticker': cmp[1], 'pages': cmp[2]}
    '''

    # Prompt for a target date
    target_date = datetime.datetime(2017, 12, 4)

    # Iterate over companies
    for constituent_id, constituent_name, url_key,pages in all_constituents:
        # Get date of latest news article for this constituent for Bloomberg
        query = """
        SELECT max(news_date) as last_date FROM `pecten_dataset.news`
        WHERE constituent_id = '{}' AND news_source = 'Bloomberg'
        """.format(constituent_id)

        try:
            result = storage_client.get_bigquery_data(query=query, iterator_flag=False)
            last_date_bq = result[0]["last_date"]
        except Exception as e:
            last_date_bq = None
            #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            #check the dates thing

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
            times = [datetime.datetime.strptime(x, '%b %d, %Y') for x in times]
            for time in times:
                if time < target_date:
                    print('Target date reached')
                    in_target = False
            for art in results:
                art_type = art.find('article')['class']
                # check if the result is an article
                if 'type-article' in art_type:
                    url = art.find('h1').find('a')['href']
                    d = get_article(url)
                    if d is None:
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

                    # get stock data
                    '''
                    try:
                        stock = get_stock_data(d['Date Published'].strip()[:-3], current_comp['ticker'])
                    except requests.exceptions.ContentDecodingError:
                        print(url)
                        continue


                    if stock is not None:
                        d['Open'] = stock['Open']
                        d['High'] = stock['High']
                        d['Low'] = stock['Low']
                        d['Close'] = stock['Close']
                        d['Volume'] = stock['Volume']
                    else:
                        d['Open'] = None
                        d['High'] = None
                        d['Low'] = None
                        d['Close'] = None
                        d['Volume'] = None
                    '''

                    # Save to database collection named after the company.
                    # Database.insert("Bloomberg_data", d)
                    pprint(d)
                    # print('Added to database.')

        break

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('python_path', help='The connection string')
    parser.add_argument('google_key_path', help='The path of the Google key')
    parser.add_argument('param_connection_string', help='The MySQL connection string')
    parser.add_argument('user', help='SOAP user')
    parser.add_argument('pwd', help='SOAP pwd')
    args = parser.parse_args()
    sys.path.insert(0, args.python_path)
    from utils.Storage import Storage, MongoEncoder
    from utils.SOAPUtils import SOAPUtils
    from utils.twitter_analytics_helpers import *
    from utils.TaggingUtils import TaggingUtils as TU
    main(args)


