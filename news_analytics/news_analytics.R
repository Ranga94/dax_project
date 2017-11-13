###############Load library################
library(mongolite)
library(sentimentr)
library(tidyverse)
library(tokenizers)

#######################Connect to Mongo DB for the collection name all_ news########## 
news_fetch <- mongo(collection = 'all_news',
                    url = "mongodb://igenie_readwrite:igenie@35.197.207.148:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())
all_news <- news_fetch$find()

#########################Categorised Tagging by passing words to be foun in the text###
title <- 0
words <- 0
title <- all_news$NEWS_TITLE_NewsDim
words <- tokenize_words(title)
tag <- 0
char <- 0
for (i in 1:length(words)){
  char <- words[[i]]
  char <- as.character(char)
  if(is.element('legal',char)){
    tag[i] <- "Legal Dispute"
  }
  else if(is.element('recall',char)){
    tag[i] <- "Product Recall"
  }
  else if(is.element('recalled',char)){
    tag[i] <- "Product Recall"
  }
  else if(is.element('recalls',char)){
    tag[i] <- "Product Recall"
  }
  else if(is.element("investment",char)){
    tag[i] <- "Investment"
  }
  else if(is.element("invests",char)){
    tag[i] <- "Investment"
  }
  else if(is.element("invested",char)){
    tag[i] <- "Investment"
  }
  else if(is.element("stock", char)){
    tag[i] <- "Stock"
  }
  else if(is.element("merger", char)){
    tag[i] <- "M&A"
  }
  else if(is.element("acquired", char)){
    tag[i] <- "M&A"
  }
  else if(is.element("acquires", char)){
    tag[i] <- "M&A"
  }
  else if(is.element("acqusition", char)){
    tag[i] <- "M&A"
  }
  else if(is.element("dividend", char)){
    tag[i] <- "Dividend"
  }
  else if(is.element("layoff", char)){
    tag[i] <- "Layoffs"
  }
  else if(is.element("layoffs", char)){
    tag[i] <- "Layoffs"
  }
  else if(is.element("laundering", char)){
    tag[i] <- "Laundering"
  }
  else if(is.element("shares", char)){
    tag[i] <- "Shares"
  }
}

table <- cbind(all_news, tag) #Combine the results into a single dataframe

#######################Sentiment Analysis###################
score <- 0
polarity <- 0

for(i in 1:nrow(all_news)){
  polarity <- sentiment(all_news$NEWS_TITLE_NewsDim[i])
  score[i] <- mean(polarity$sentiment)
}

positive <- 0.2                     ###Positive > 0.2
sentiment <- 0                      ###Negative < 0
                                    ###Neutral: 0 to 0.2
for(i in 1:length(score)){
  if(score[i]<0) {
    sentiment1="negative"
  } else if (score[i]>= positive) {
    sentiment1="positive"
  } else{
    sentiment1="neutral"
  }
  sentiment[i]=sentiment1
}


table <- cbind(table, score, sentiment) #Combine the result to single dataframe


############################Insert back to Mongo DB#################
news_fetch$insert(table)
