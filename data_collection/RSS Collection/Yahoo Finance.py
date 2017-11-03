import feedparser
from time import gmtime, strftime
from urllib.request import urlopen
import json

d = feedparser.parse('http://finance.yahoo.com/rss/headline?s=ADS.DE') # replace ADS.DE with ticker symbol any other constituent

for post in d.entries:
    print (post.published + ", " + post.title + ", " + post.link + " ") # print all entries and their links

with open('insert diretory of output file', 'a') as outfile:
    json.dump(d.entries, outfile)