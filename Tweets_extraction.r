#install.pacakages("twitteR")
#install.packages("ROAuth")

library(twitteR)
library(ROAuth)

api_key <- " " #Enter the api key within the quotes
api_secret <- " " #Enter the api secret key within quotes
access_token <- " " #Enter the access token within quotes
access_token_secret <- " " #Enter access token secret within quotes
setup_twitter_oauth(api_key,api_secret,access_token,access_token_secret)

searchString <- "#BMW" #Enter the string to bea serached for within the quotes
numOfTweets <- 2000 #Enter the number of tweets 
lang <- "en"
tweetSince <- '2017-07-20' #Enter the start date 
tweetUntil <- '2017-07-21' #Enter the end date
tweetsData <-searchTwitter(searchString,n=numOfTweets,lang=lang,since=tweetSince,until=tweetUntil)
tweetsDF <- twListToDF(tweetsData)
#head(tweetsDF)
write.csv(tweetsDF, file = "BMW_20.csv", row.names = FALSE) #Enter the file name within the quotes to be saved to within the quotes
