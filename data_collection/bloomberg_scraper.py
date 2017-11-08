from bs4 import BeautifulSoup
import re, datetime, requests, pandas_datareader
import pandas as pd
from database import Database
from pandas_datareader import data
from time import sleep
from fake_useragent import UserAgent


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

    return {'Title': title, 'Content': news_text, 'Date Published': published, 'Sources': source}


# Define random user agent object
ua = UserAgent()

# Initialize database
Database.initialize()

# Base url for search queries
base = 'https://www.bloomberg.com/search?query={}&page={}'

# Read csv file in format: Company Name,Ticker Symbol,Number of Pages to a dictionary
companies = {}
with open('companies.csv', 'r') as f:
    lines = f.read().split('\n')
    for line in enumerate(lines, 1):
        cmp = line[1].split(',')
        companies[line[0]] = {'name': cmp[0].replace('.', '_').replace(' ', '_'), 'ticker': cmp[1], 'pages': cmp[2]}

# Print company names and their key
for c in companies.keys():
    print(c, companies[c]['name'])

# Prompt for a number of a company to scrape
while True:
    try:
        number = int(input('Enter a number of company to scrape: '))
    except ValueError:
        print('You have to pass an integer.')
        continue
    if number > len(companies.keys()) or number < 1:
        print("You have to enter a number coresponding to one of the companies. ")
        continue
    current_comp = companies[number]
    break

print('Scraping for {}.\nThere is {} pages available to scrape.'.format(current_comp['name'], current_comp['pages']))
# Prompt for starting page and end page of scraping.
while True:
    try:
        start = int(input("Enter a start page: "))
    except ValueError:
        print('You have to pass an integer.')
    if start < 1 or start > int(current_comp['pages']):
        print('You have enter a number larger then 1 and lower then number of pages available to scrape.')
        continue
    break

while True:
    try:
        end = int(input("Enter an end page: "))
    except ValueError:
        print('You have to pass an integer.')
    if end < 1 or end > int(current_comp['pages']):
        print('You have enter a number larger then 1 and lower then number of pages available to scrape.')
        continue
    end += 1
    break

print("Starting scraping from {} to {} page".format(start, end-1))

# iterate through specified page range and save articles to database
for i in range(start, end):
    print('Scraping for page',i)
    # format url with company name and current page number and create bs4 object from the response
    url = base.format(current_comp['name'], i)
    main_soup = BeautifulSoup(requests.get(url, headers={'User-Agent': ua.random}).content, 'html.parser')

    # check if there are any results on the page and finish the loop if none
    try:
        if 'No results for' in main_soup.find('div', {'class': 'content-no-results'}).text:
            print('No more articles...')
            break
    except AttributeError:
        pass

    # iterate through article elelements on the page
    for art in main_soup.findAll('div', {'class': 'search-result'}):
        art_type = art.find('article')['class']
        # check if the result is an article
        if 'type-article' in art_type:
            url = art.find('h1').find('a')['href']
            d = get_article(url)
            if d is None:
                continue
            # get stock data
            stock = get_stock_data(d['Date Published'].strip()[:-3], current_comp['ticker'])
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

            # Save to database collection named after the company.
            Database.insert(current_comp['name'], d)
            print('Added to database.')