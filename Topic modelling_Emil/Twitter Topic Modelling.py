import langid
import nltk
import re
import time
from collections import defaultdict
from configparser import ConfigParser
from gensim import corpora, models, similarities
from nltk.tokenize import RegexpTokenizer
from pymongo import MongoClient
from string import digits

# create definitions to read the twitter data

def filter_lang(lang, documents):
    doclang = [  langid.classify(doc) for doc in documents ]
    return [documents[k] for k in range(len(documents)) if doclang[k][0] == lang]

# connect to the database, retrieve data and filter for Commerzbank tweets in English

client = MongoClient('mongodb://igenie_readwrite:igenie@35.197.222.38:27017,35.197.238.147:27017/dax_gcp?replicaSet=rs0')
db = client.dax_gcp
documents = [tweets['text'] for tweets in db.tweets.find()
            if (tweets['lang'] in ('en')) and (tweets['constituent'] in ('insert constituent here'))]

print("We have " + str(len(documents)) + " tweets in english ")

# Remove urls
documents = [re.sub(r"(?:\@|http?\://)\S+", "", doc)
                for doc in documents ]

# Remove documents with less 100 words (some timeline are only composed of URLs)
documents = [doc for doc in documents if len(doc) > 100]

# tokenize
from nltk.tokenize import RegexpTokenizer

tokenizer = RegexpTokenizer(r'\w+')
documents = [ tokenizer.tokenize(doc.lower()) for doc in documents ]

# Remove stop words
stoplist_tw=['amp','get','got','hey','hmm','hoo','hop','iep','let','ooo','par',
            'pdt','pln','pst','wha','yep','yer','aest','didn','nzdt','via',
            'one','com','new','like','great','make','top','awesome','best',
            'good','wow','yes','say','yay','would','thanks','thank','going',
            'new','use','should','could','best','really','see','want','nice',
            'while','know','https']

unigrams = [ w for doc in documents for w in doc if len(w)==1]
bigrams  = [ w for doc in documents for w in doc if len(w)==2]

nltk.download('stopwords')

stoplist  = set(nltk.corpus.stopwords.words("english") + stoplist_tw
                + unigrams + bigrams)
documents = [[token for token in doc if token not in stoplist]
                for doc in documents]

# rm numbers only words
documents = [ [token for token in doc if len(token.strip(digits)) == len(token)]
                for doc in documents ]

# Lammetization
# This did not add coherence ot the model and obfuscates interpretability of the
# Topics. It was not used in the final model.
#   from nltk.stem import WordNetLemmatizer
#   lmtzr = WordNetLemmatizer()
#   documents=[[lmtzr.lemmatize(token) for token in doc ] for doc in documents]

# Remove words that only occur once
token_frequency = defaultdict(int)

# count all token
for doc in documents:
    for token in doc:
        token_frequency[token] += 1

# keep words that occur more than once
documents = [ [token for token in doc if token_frequency[token] > 1]
                for doc in documents  ]

# Sort words in documents
for doc in documents:
    doc.sort()

# Build a dictionary where for each document each word has its own id
dictionary = corpora.Dictionary(documents)
dictionary.compactify()
# and save the dictionary for future use
dictionary.save('CB.dict')

# We now have a dictionary with 26652 unique tokens
print(dictionary)

# Build the corpus: vectors with occurence of each word for each document
# convert tokenized documents to vectors
corpus = [dictionary.doc2bow(doc) for doc in documents]

# and save in Market Matrix format
corpora.MmCorpus.serialize('CB.mm', corpus)
# this corpus can be loaded with corpus = corpora.MmCorpus('alexip_followers.mm')                

lda_filename = 'CB.lda'

lda_params = {'num_topics': 40, 'passes': 100, 'alpha': 0.001}

lda = models.LdaModel(corpus, id2word=dictionary,
                        num_topics=lda_params['num_topics'],
                        passes=lda_params['passes'],
                        alpha = lda_params['alpha'])

lda.print_topics()

import pyLDAvis.gensim

corpus = corpora.MmCorpus('data/CB.mm')

dictionary = corpora.Dictionary.load('data/CB.dict')

lda = models.LdaModel.load('CB.lda')

data =  pyLDAvis.gensim.prepare(lda, corpus, dictionary)

pyLDAvis.display(data)
