library(tm)

library(e1071)
library(gmodels)
library(caret)

#######################Read the file###################################
tweet_raw <- read.csv(file.choose(), header=TRUE, stringsAsFactors = FALSE) ###This is to read the file
str(tweet_raw) #Summary of what is read
tweet_raw <- tweet_raw[sample(1:nrow(tweet_raw)), ] #Randomize the data set
#Converting "type" to factor
tweet_raw$relavance <- factor(tweet_raw$relavance)
table(tweet_raw$relavance)
#################Data Preparartion and Cleaning and standardizing text data###################
#Since the data is already loaded, 
#using the VectorSource() reader function to create a source object from the existing
#tweet_raw$text vector, which can then be supplied to VCorpus()
tweet_corpus <- VCorpus(VectorSource(tweet_raw$Tweet))

print(tweet_corpus)

#Convert the Rawtext to lower case
tweet_corpus_clean <- tm_map(tweet_corpus, content_transformer(tolower))

#Remove Numbers
tweet_corpus_clean <- tm_map(tweet_corpus_clean, removeNumbers)

#Removing Stop words
tweet_corpus_clean <- tm_map(tweet_corpus_clean, removeWords, stopwords())

#Remove Punctuation
tweet_corpus_clean <- tm_map(tweet_corpus_clean, removePunctuation)

#The stemming process takes words like learned,
#learning, and learns, and strips the suffix in order to transform them into the base
#form, learn.
tweet_corpus_clean <- tm_map(tweet_corpus_clean, stemDocument)

#Remove Whitespace
tweet_corpus_clean <- tm_map(tweet_corpus_clean, stripWhitespace)

##########Splitting text documents into words###########
#Create Document Term Matrix
tweet_dtm <- DocumentTermMatrix(tweet_corpus_clean)

###########Creating and prepairing Training and Test Data sets################
tweet_dtm_train <- tweet_dtm[1:1900, ] #Enter the number for the training dataset
tweet_dtm_test <- tweet_dtm[1901:2287, ] #Enter the number for the testing dataset

tweet_train_labels <- tweet_raw[1:1900, ]$Relavance #Enter the same number as above
tweet_test_labels <- tweet_raw[1901:2287, ]$Relavance 

prop.table(table(tweet_train_labels))
prop.table(table(tweet_test_labels))


#######################Creating Indiacator Features for frequent words###########
#This function takes a DTM and returns a character vector containing
#the words that appear for at least the specified number of times. For instance,
##the following command will display the words appearing at least five times in
#the tweet_dtm_train matrix:
tweet_freq_words <- findFreqTerms(tweet_dtm_train, 5) 
str(tweet_freq_words)

#now to filter our DTM to include only the terms appearing in a specified
#vector. As done earlier, using the data frame style [row, col] operations to
#request specific portions of the DTM, noting that the columns are named after the
#words the DTM contains. We can take advantage of this to limit the DTM to specific
#words. Since we want all the rows, but only the columns representing the words in
#the sms_freq_words vector, our commands are:

tweet_dtm_freq_train<- tweet_dtm_train[ , tweet_freq_words]
tweet_dtm_freq_test <- tweet_dtm_test[ , tweet_freq_words]

#The Naive Bayes classifier is typically trained on data with categorical features.
#This poses a problem, since the cells in the sparse matrix are numeric and measure
#the number of times a word appears in a tweet. We need to change this to a
#categorical variable that simply indicates yes or no depending on whether the
#word appears at all.
#The following defines a convert_counts() function to convert counts to
#Yes/No strings:
convert_counts <- function(x) {
  x <- ifelse(x > 0, "Yes", "No")
}

#The ifelse(x > 0, "Yes", "No") statement transforms
#the values in x, so that if the value is greater than 0, then it will be replaced by "Yes",
#otherwise it will be replaced by a "No" string. Lastly, the newly transformed x vector
#is returned.
#We now need to apply convert_counts() to each of the columns in our sparse
#matrix.

#The apply() function allows a function to be used on each of the rows or columns
#in a matrix. It uses a MARGIN parameter to specify either rows or columns. Here,
#we'll use MARGIN = 2, since we're interested in the columns (MARGIN = 1 is used
#                                                             for rows). 
#The commands to convert the training and test matrices are as follows:

tweet_train <- apply(tweet_dtm_freq_train, MARGIN = 2, convert_counts)
tweet_test <- apply(tweet_dtm_freq_test, MARGIN = 2, convert_counts)

#To build our model on the tweet_train matrix, we'll use the following command:
tweet_classifier <- naiveBayes(tweet_train, tweet_train_labels)

#######################Evaluating the Model################
tweet_test_pred <- predict(tweet_classifier, tweet_test)


CrossTable(tweet_test_labels, tweet_test_pred, prop.chisq = FALSE, prop.t = FALSE, dnn = c('actual', 'predict'))
conf.mat <- confusionMatrix(tweet_test_pred, tweet_test_labels)
conf.mat
