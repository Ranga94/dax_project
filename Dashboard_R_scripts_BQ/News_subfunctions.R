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

##This plots a bar chart to display the number of news under any tags. 
count_tags_bar<-function(news_df,constituent){
  title_str = paste('Number of News for ',constituent,' by Categories', sep='')
  df<-news_df[news_df$Constituent==constituent,c('Tags','Count')]
  df<-df[rowSums(is.na(df)) == 0,]  
  df<-df[order(df$Count,decreasing=T)[1:5],]
  ggplot(df,aes(x=df$Tag,y=df$Count,fill=df$Tag))+geom_col()+labs(y="Count of news",x="News tagging") +
    geom_text(aes(label=df$Count),hjust=0.5,vjust=0)+ggtitle(title_str) +scale_fill_discrete(guide=FALSE)+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
}


##This plots a line graph displaying the daily News Sentiment
daily_news_sent<-function(df,constituent){
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


## News Box
news_analytics_topic_articles_func <-function(param_db,constituent){
  #db$NEWS_DATE_NewsDim<- as.Date(db$NEWS_DATE_NewsDim,format='%d/%m/%Y')
  db <- param_db[param_db$constituent == constituent,]
  db<- db[order(-as.numeric(db$NEWS_DATE_NewsDim)),] ##order by release dates, descending
  num_news<-nrow(db)
  db <- db[1:num_news,c('NEWS_TITLE_NewsDim','constituent','categorised_tag')]
  
  #make sure the news link only contains 8 characters from the headline. 
  db$NEWS_TITLE_NewsDim <- as.character(db$NEWS_TITLE_NewsDim)
  db$NEWS_TITLE_NewsDim <- unlist(lapply(db$NEWS_TITLE_NewsDim, string_fun)) ##Apply the limit
  ##remove the NA
  db$NEWS_TITLE_NewsDim <-gsub("NA", " ", db$NEWS_TITLE_NewsDim)
  
  df<- datatable(db[,c('NEWS_TITLE_NewsDim','constituent','categorised_tag')],rownames=FALSE, options = list(pageLength = 5),colnames = c('Headline' ,'Constituent','Topic'),escape=FALSE)
}


topic_sentiment_grid_plot<-function(topic_sentiment_df,constituent){
  df<-topic_sentiment_df[topic_sentiment_df$constituent==constituent,]
  ##Only extract the first 10 popular topics
  df<- df[order(-as.numeric(df$count)),]
  
  if(nrow(df)>=15){
    n=15
  }else if(nrow(df)>=12){
    n=12
  }else if(nrow(df)>=9){
    n=9
  }else if(nrow(df)>=6){
    n=6
  }else{
    n=nrow(df)}
  
  ##Set size, sentiment color etc
  df<-df[df$categorised_tag!='None',]
  df<-df[1:n,]
  df$size = 1
  df$sentiment_position=1
  df[df$overall_sentiment=='positive',c('sentiment_position')] = 1
  df[df$overall_sentiment=='negative',c('sentiment_position')] = -1
  
  sentiment_array<-unique(df$overall_sentiment)
  
  if((sentiment_array=='positive')&(length(sentiment_array)==1)){
    p<-treemap(df,index="categorised_tag",vSize="size",
               vColor = "count",
               type='manual',
               palette = "Greens",
               algorithm = 'squarified',
               fontsize.labels=c(10,10),
               #title="News Categories Sentiment over the Last 2 Months",
               fontsize.title = c(0),
               fontface.labels=c(1,1),
               #bg.labels=c("transparent"), 
               border.col=c("white","white"), 
               border.lwds=c(7,2),
               align.labels=list(
                 c("center", "center"), 
                 c("right", "bottom")
               ),      
               title.legend="All Positive News Count: Low to High",
               overlap.labels=0.5,
               inflate.labels=F)
    
  }else if((sentiment_array=='negative')&(length(sentiment_array)==1)){
    p<-treemap(df,index="categorised_tag",vSize="size",vColor = "count",
               type='manual',
               algorithm = 'squarified',
               palette="Reds",
               fontsize.labels=c(10,10),
               #title="News Categories Sentiment over the Last 2 Months",
               fontsize.title = c(0),
               fontface.labels=c(1,1),
               #bg.labels=c("transparent"), 
               border.col=c("white","white"), 
               border.lwds=c(7,2),
               align.labels=list(
                 c("center", "center"), 
                 c("right", "bottom")
               ),      
               title.legend="All Negative News Count: Low to High",
               overlap.labels=0.5,
               inflate.labels=F)
  }else{
    p<-treemap(df,index="categorised_tag",vSize="size",
               vColor = "sentiment_position",
               type='manual',
               algorithm = 'squarified',
               palette=c("red","#1E8449"),
               mapping=c(-1,1),
               fontsize.labels=c(10,10),
               #title="News Categories Sentiment over the Last 2 Months",
               fontsize.title = c(0),
               fontface.labels=c(1,1),
               #bg.labels=c("transparent"), 
               border.col=c("white","white"), 
               border.lwds=c(7,2),
               align.labels=list(
                 c("center", "center"), 
                 c("right", "bottom")
               ),      
               title.legend="News Sentiment: Negative to Positive",
               overlap.labels=0.5,
               inflate.labels=F)
  }
  
  p
}