####################This is to count the frequency of terms using tm package##########
library(tm) #Install the tm package as 'install.packages("tm")'
review_source <-VectorSource(messages) #Where "messages" contains the text to be mined 
corpus <- Corpus(review_source)

corpus <- tm_map(corpus, content_transformer(tolower)) #Converting the text to  lowercase 
corpus <- tm_map(corpus, removePunctuation) #Remove Punctuations
corpus <- tm_map(corpus, stripWhitespace) 
corpus <- tm_map(corpus, removeWords, stopwords("english")) #remove English Stopwords

dtm <- DocumentTermMatrix(corpus) #Convert it to document term matrix
dtm2 <- as.matrix(dtm) #Convert it to matrix
frequency <- colSums(dtm2)
frequency <- sort(frequency, decreasing=TRUE)[1:10] #Change the number in bracket to get top most repeated words
frequency #Frequency has the most frequently repeated words in decreasing order