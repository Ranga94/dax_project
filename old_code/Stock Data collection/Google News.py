import feedparser
from time import gmtime, strftime
from urllib.request import urlopen
import json

d = feedparser.parse('insert url of constituent RSS feed') 

for post in d.entries:
    print (post.published + ", " + post.title + ", " + post.link + " ") # print all entries and their links

with open('directory of output file', 'a') as outfile:
    json.dump(d.entries, outfile)