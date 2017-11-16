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
title_str = paste('News Tagging Count of ',constituent, sep='')
if (constituent=='Deutsche Bank'){
  df<-news_df[news_df$constituent==(constituent),c('tag','count')]}else{
    df<-news_df[news_df$constituent==tolower(constituent),c('tag','count')] }

ggplot(df,aes(x=df$tag,y=df$count,fill=df$tag))+geom_col()+labs(y="Count of news",x="News tagging") +
  geom_text(aes(label=df$count),hjust=0.5,vjust=0)+ggtitle(title_str) +scale_fill_discrete(guide=FALSE)+
  theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
}


##This plots a line graph displaying the daily News Sentiment
daily_news_sent<-function(url_mongo,constituent){
  if(constituent=='Adidas'){
    daily_news <- mongo(collection = 'news_daily_sent_adidas',
                      url = url_mongo,
                      verbose = FALSE, options = ssl_options())
  }

  if(constituent=='BMW'){
   daily_news <- mongo(collection = 'news_daily_sent_bmw',
                      url = url_mongo,
                      verbose = FALSE, options = ssl_options())
  }

  if(constituent=='Commerzbank'){
    daily_news <- mongo(collection = 'news_daily_sent_commerzbank',
                      url = url_mongo,
                      verbose = FALSE, options = ssl_options())
  }
  if(constituent=='Deutsche Bank'){  
    daily_news <- mongo(collection = 'news_daily_sent_deutsche_bank',
                      url = url_mongo,
                      verbose = FALSE, options = ssl_options())}

  if(constituent=='EON'){
    daily_news <- mongo(collection = 'news_daily_sent_eon',
                      url = url_mongo,
                      verbose = FALSE, options = ssl_options())
  }

  df<-daily_news$find()
  df$date <- as.Date(df$date,"%Y-%m-%d")

  ggplot(data=df, aes(x=df$date, y=df$scorescore,group=1))+geom_line()+
    geom_point()+
    labs(y="Average news sentiment",x="Date")+
    ylim(-1,1)+
    labs(title = paste("Daily News Sentiment for ",constituent,sep=""))+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
    
  }

news_sent_by_tag<-function(news_tag_df,constituent){
  if(constituent=='Deutsche Bank'){df<-news_tag_df[news_tag_df$constituent==constituent,]}else{
    df<-news_tag_df[news_tag_df$constituent==tolower(constituent),]}
  #df$date<-gsub("-", "/", df$date)
  df$date <- as.Date(df$date,"%Y-%m-%d")
  ##Only focus on shares, investment and stock related news
  title_str = paste('Daily Average News Sentiment for ', constituent, ' by Categories',sep='')
  #df_selected = df[(df$categorised_tag=='Stock'|df$categorised_tag=='Investment'|df$categorised_tag=='Shares'),]
  ggplot(data = df,aes(x=df$date,y=df$scorescore,group=df$categorised_tag)) +
    scale_y_continuous(limits=c(-1,1))+
    geom_point(aes(color=categorised_tag))+geom_line(aes(color=categorised_tag))+
    ggtitle(title_str)+labs(y="Daily average sentiment score",x="Date")+
    scale_fill_discrete(name = "News Category")+
    theme(legend.position="bottom",legend.direction='horizontal')+
    labs(colour = "Category") +
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
}

