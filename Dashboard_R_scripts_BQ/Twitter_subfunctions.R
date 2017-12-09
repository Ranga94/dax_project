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


###For Twitter Sentiment Count, we need a dictionary to match constituent_name with constituent
constituents_array = c('Merck','Daimler','Bayer','Allianz','Adidas','BMW','Commerzbank','Deutsche Bank','EON',
                       'SAP','Continental','Siemens','Lufthansa','BASF','Thyssenkrupp','Infineon', 'Linde',
                       'RWE','Deutsche Telekom','Fresenius Medical Care','Fresenius','Deutsche Post','Deutsche Börse',
                       'Beiersdorf','Vonovia','Volkswagen','ProSiebenSat1 Media','HeidelbergCement','Henkel')

constituent_names_array<-c('MERCK KGAA','DAIMLER AG','BAYER AG','ALLIANZ SE','ADIDAS AG','BAYERISCHE MOTOREN WERKE AG','COMMERZBANK AKTIENGESELLSCHAFT','DEUTSCHE BANK AG',
                           'E.ON SE','SAP SE','CONTINENTAL AG','SIEMENS AG','DEUTSCHE LUFTHANSA AG','BASF SE','THYSSENKRUPP AG',
                           'INFINEON TECHNOLOGIES AG','LINDE AG','RWE AG','DEUTSCHE TELEKOM AG','FRESENIUS MEDICAL CARE AG & CO.KGAA',
                           'FRESENIUS SE & CO.KGAA','DEUTSCHE POST AG','DEUTSCHE BOERSE AG','BEIERSDORF AG',
                           'VONOVIA SE','VOLKSWAGEN AG','PROSIEBENSAT.1 MEDIA SE','HEIDELBERGCEMENT AG','HENKEL AG & CO. KGAA')

names(constituent_names_array)<-constituents_array 


##This stores functions used for the Twitter Page of Analytics Dashboard.
#This plots a bar chart for the general target price

## General Twitter target prices
general_target_price_bar<-function(df,constituent){
  df<-df[df$constituent_name==constituent_names_array[[constituent]],]
  if(mean(df$price)>=100){space=5}
  if(mean(df$price)<100){space=2}
  df$price <- as.numeric(df$price)
  title_str<-paste('General Target Price Distribution for ',constituent, sep='')
  ggplot(data=df, aes(x=df$price,y = (..count..)/sum(..count..)))+
    geom_histogram(breaks=seq(min(df$price-space), max(df$price+space), by=space), col="white", aes(fill=100*(..count..)/sum(..count..))) +
    scale_y_continuous(labels = scales::percent)+
    scale_fill_gradient("% Tweets", low="#D7BDE2", high="#9B59B6")+
    labs(title=title_str,x='Target Price €',y='% Tweets')+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
}

## Influencer target prices
influencer_target_price_bar<-function(df,constituent){
  df<-df[df$constituent_name==constituent_names_array[[constituent]],]
  if(mean(df$price)>=100){space=5}
  if(mean(df$price)<100){space=2}
  df$price <- as.numeric(df$price)
  title_str<-paste('Influencer Target Price Distribution for ',constituent, sep='')
  ggplot(data=df, aes(x=df$price,y = (..count..)/sum(..count..)))+
    geom_histogram(breaks=seq(min(df$price-space), max(df$price+space), by=space), col="white", aes(fill=100*(..count..)/sum(..count..))) +
    scale_y_continuous(labels = scales::percent)+
    scale_fill_gradient("% Tweets", low="#EDBE94", high="#E86E00")+
    labs(title=title_str,x='Target Price €',y='% Tweets')+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
}



#This function counts the number of positive, negative and neutral tweets for one specific stock. 
tweet_count<-function(twitter_counts,constituent){
  constituent_count <- twitter_counts[twitter_counts$constituent_name==constituent_names_array[[constituent]],]
  
  #if(constituent =='Adidas'){
  # constituent_count <- twitter_counts[twitter_counts$constituent=='adidas',]
  #}else{constituent_count <- twitter_counts[twitter_counts$constituent==constituent,]}
  
  pos_line = constituent_count[constituent_count$line=='Positive',c("count")]
  neg_line = constituent_count[constituent_count$line=='Negative',c("count")]
  neu_line = constituent_count[constituent_count$line=='Neutral',c("count")]
  
  n=max(length(pos_line),length(neu_line),length(neg_line))
  if (length(pos_line)<n){pos_line<-append(pos_line,rep(0,n-length(pos_line)),after=min(pos_line))}
  if (length(neu_line)<n){neu_line<-append(neu_line,rep(0,n-length(neu_line)),after=min(neu_line))}
  if (length(neg_line)<n){neg_line<-append(neg_line,rep(0,n-length(neg_line)),after=min(neg_line))}
  
  title_str = paste('Number of Tweets for ',constituent,sep='')
  
  #if (constituent =='EON'){
  #neg_line <- append(neg_line,0,after=min(neg_line))}
  date_range <-unique(constituent_count$date)
  date_range <- as.Date(date_range,"%Y-%m-%d")
  date_range<-gsub("-", "/", date_range)
  date_range <- as.Date(date_range,"%Y/%m/%d")
  
  #To short-fix inconsistent record
  if (length(date_range)>length(pos_line)){
    pos_line<-append(pos_line,rep(0,length(date_range)-length(pos_line)),after=min(pos_line))
    neu_line<-append(neu_line,rep(0,length(date_range)-length(neu_line)),after=min(neu_line))
    neg_line<-append(neg_line,rep(0,length(date_range)-length(neg_line)),after=min(neg_line))
  }
  
  #constituent_count_posi<-constituent_count[constituent_count$line=='Positive',c("count",'date')]
  #data=constituent_count_posi
  ggplot()+
    geom_line(aes(x=date_range,y=pos_line,group=1,color="Positive")) +geom_point(aes(x=date_range,y=pos_line,group=1,color="Positive"))+
    geom_line(aes(x=date_range,y=neg_line,group=1,color="Negative")) +geom_point(aes(x=date_range,y=neg_line,group=1,color="Negative"))+
    geom_line(aes(x=date_range,y=neu_line,group=1,color="Neutral")) +geom_point(aes(x=date_range,y=neu_line,group=1,color="Neutral"))+
    ggtitle(title_str)+labs(y="Number of Tweets",x="Date")+
    scale_colour_manual(name="Sentiment",values=c("Positive"="#1E8449","Neutral"="#FFCC00","Negative"="red"))+
    theme(legend.position="bottom",legend.direction='horizontal')+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
}


##This function creates a color-coded world map according to Tweet Frequency
map_frequency<-function(country_df,constituent){
  #title_str = paste('Tweeting frequency for ',constituent,' by countries',sep='')
  df<-country_df[country_df$constituent_name==constituent_names_array[[constituent]],c("count","avg_sentiment",'country_name')]
  n <- joinCountryData2Map(df, joinCode="ISO2", nameJoinColumn="country_name")
  par(mar=c(0,0,0,0))
  mapCountryData(n, nameColumnToPlot="count", mapTitle='',colourPalette=rwmGetColours(c('#F5EEF8','#D7BDE2','#9B59B6','#7D3C98'),4))
}


##This function creates a color-coded world map according to Twitter Sentiment
##Find the minimum/maximum of the sentiment, in order to decide the range of colors.
map_sentiment<-function(country_df,constituent){
  #title_str = paste('Twitter sentiment for ',constituent,' by countries',sep='')
  df<-country_df[country_df$constituent_name==constituent_names_array[[constituent]],c("count","avg_sentiment",'country_name')]
  min_sent<-min(df$avg_sentiment, na.rm=T)
  max_sent<-max(df$avg_sentiment, na.rm=T)
  if ((min_sent<=0)&(max_sent>=0)){palette_color = c('red','#EB984E','#FFCC00','#1E8449')}
  if ((min_sent<=0)&(max_sent<=0)){palette_color = c('red','#EB984E','#FFCC00')}
  if ((min_sent>=0)&(max_sent>=0)){palette_color = c('#FFCC00','#1E8449')}
  
  n <- joinCountryData2Map(df, joinCode="ISO2", nameJoinColumn="country_name")
  par(mar=c(0,0,0,0))
  mapCountryData(n, nameColumnToPlot="avg_sentiment", mapTitle='',colourPalette=rwmGetColours(palette_color,length(palette_color)))
}


recent_tweets<-function(retrieved_data,constituent){
  df<-retrieved_data[retrieved_data$constituent_name ==constituent_names_array[[constituent]],]
  
  table<- datatable(df[,c('text','sentiment_score')],rownames=FALSE, options = list(pageLength = 5),colnames = c('Tweet','Sentiment'),escape=FALSE) %>%
    formatStyle('sentiment_score',
                color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
                backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))
}