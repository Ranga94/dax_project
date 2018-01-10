
library(pander)
library(markdown)
library(stringr)
library(mongolite)
library(reshape)
library(reshape2)
library(tidyverse)
library(ggplot2)
library(rworldmap)
library(wordcloud)
library(RColorBrewer)
library(plotly)
library(plyr)

##This stores functions used for the News Page of Analytics Dashboard.
##Plot a word-cloud

word_cloud_plot<-function(top_words_df,constituent){
  if(constituent=='Adidas'){
    df<-top_words_df[top_words_df$Constituent==tolower(constituent),c('Words','Frequency')]}else{
      df<-top_words_df[top_words_df$Constituent==(constituent),c('Words','Frequency')]}
  
  d <- tail(df[,c('Words','Frequency')])
  set.seed(1234)
  wordcloud(words = d$Words, freq = d$Frequency, min.freq = 1,
            max.words=200, random.order=FALSE, rot.per=0.35,
            colors=brewer.pal(8, "Dark2"))}


##This plots a bar chart to display the number of news under any tags. 
count_tags_bar<-function(news_df,constituent){
  
  
  title_str = paste('Number of News for ',constituent,' by Categories', sep='')
  df<-news_df[news_df$constituent==(constituent),c('tags','count')]
  df<-df[rowSums(is.na(df)) == 0,]  
  df<-df[order(df$count,decreasing=T)[1:5],]
  ggplot(df,aes(x=df$tag,y=df$count,fill=df$tag))+geom_col()+labs(y="Count of news",x="News tagging") +
    geom_text(aes(label=df$count),hjust=0.5,vjust=0)+ggtitle(title_str) +scale_fill_discrete(guide=FALSE)+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
}


##This plots a line graph displaying the daily News Sentiment
daily_news_sent<-function(url_mongo,constituent){
  daily_news <- mongo(collection = 'news_analytics_daily_sentiment',
                          url = url_mongo,
                          verbose = FALSE, options = ssl_options())
  
  df<-daily_news$find()
  df<-df[df$constituent==constituent,]
  df$date <- as.POSIXct(df$date)
  df<-df[df$date>as.Date('2017-10-01'),]
  ggplot(data=df, aes(x=df$date, y=df$avg_sentiment,group=1))+geom_line(size=1,color='#4f9dd6')+
    geom_point(color='#4f9dd6')+
    labs(y="Average news sentiment",x="Date")+
    ylim(-1,1)+
    labs(title = paste("Daily News Sentiment for ",constituent,sep=""))+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
  
}

news_sent_by_tag<-function(tag_score_df,constituent){
  
  df_score<-tag_score_df[tag_score_df$constituent==constituent,]
  
  title_string<- paste('Daily News Sentiment by News Categories for ',constituent, sep='')
  num = 30
  ##only want to display the tags that appear on more than certain(n) dates. 
  if(constituent=='Adidas'||constituent=='EON'){num=15}
  if(constituent =='BMW'){num=44}
  if(constituent=='Commerzbank'){num=26}
  if(constituent=='Deutsche Bank'){num=34}
  
  newd <-  df_score %>% group_by(categorised_tag) %>% filter(n()>num)
  newd <-newd[newd$categorised_tag!='None',]
  
  ggplot(data = newd,aes(x=newd$date,y=newd$avg_sentiment,group=newd$categorised_tag)) +
    scale_y_continuous(limits=c(-1,1))+
    geom_point(aes(color=categorised_tag))+geom_line(aes(color=categorised_tag))+
    ggtitle(title_string)+labs(y="Daily average sentiment score",x="Date")+
    scale_fill_discrete(name = "News Category")+
    theme(legend.position="bottom",legend.direction='horizontal')+
    labs(colour = "Category") +
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
  
}
