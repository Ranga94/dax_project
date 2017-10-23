library(sentimentr)

table_data <- read.csv(file.choose(), header = TRUE, stringsAsFactors = FALSE)
#######################Check for emoticon and replace it with words####################
for(i in 1:length(text)){
  temp <- replace_emoticon(text[i])
  text[i] <- temp
}
#text <- as.data.frame(text)

##########################Compute the polarity scores for the text################
score <- 0
for(i in 1:length(text)){
  polarity = sentiment(text[i])
  score[i]=sum(polarity$sentiment)
  
}
score <- as.numeric(score)

########################Defining the emotion range####################
positive <- 0.2 ############To set the sentiment postive for a score more than 0.2

#######################Classify the emotion according to polarity scores###########
sentiment <- 0
for(i in 1:length(score)){
  if(score[i]==0) {
    sentiment1="neutral"
  } else if (score[i]> positive) {
    sentiment1="positive"
  } else{
    sentiment1="negative"
  }
  sentiment[i]=sentiment1
}

computed_sentiment <- as.character(sentiment)
table_data <- cbind(table_data, sentiment)
write.csv(table_data, file = "sentiment.csv", row.names = FALSE) #Write a csv
