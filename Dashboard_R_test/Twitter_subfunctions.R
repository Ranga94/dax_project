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


##This stores functions used for the Twitter Page of Analytics Dashboard.
#This plots a bar chart for the general target price
general_target_price_bar<-function(df,constituent){
  if(constituent=='Adidas'){
    price<- df[df$constituent==tolower(constituent),c('name','value')]}else{
      price<- df[df$constituent==constituent,c('name','value')]}  
  price$name <- as.numeric(price$name)
  price$value <- as.numeric(price$value)
  mean = round(sum(price$name * price$value /100),2)
  ggplot(price,aes(x=name,y=value),main='Target Price Distribution')+geom_bar(position = "dodge",stat = "identity",fill = 'orange')+
    labs(y="% Twitter population",x="Target price (€)") +
    geom_text(aes(label=ifelse(value>=5, paste('€',name, sep=''),'')),hjust=0.5,vjust=0)}

#This plots a bar chart for the influencer target price
influencer_target_price_bar<-function(df2,constituent){
  if(constituent=='Adidas'){
    price<- df2[df2$constituent==tolower(constituent),c('name','value')]}else{
      price<- df2[df2$constituent==input$constituent,c('name','value')]}
  price$name <- as.numeric(price$name)
  price$value <- as.numeric(price$value)
  mean = round(sum(price$name * price$value /100),2)
  ggplot(price,aes(x=name,y=value),main='Target Price Distribution')+geom_bar(position = "dodge",stat = "identity",fill = '#7D6FF0') +
    labs(y="% Twitter population",x="Target price (€)") +
    geom_text(aes(label=ifelse(value>=5, paste('€',name, sep=''),'')),hjust=0.5,vjust=0)}


#This makes a bar chart displaying the top words mentioned in relevant tweets
top_mentions_bar<-function(db_organization,constituent){
  if(constituent=='Adidas'){
    db<-db_organization[db_organization$constituent==tolower(constituent),c('trend','count')]}else{
      db<-db_organization[db_organization$constituent==(constituent),c('trend','count')]}
  db<-db[!(db$trend==constituent),]
  db<-db[!(db$trend==tolower(constituent)),]
  db<-db[rowSums(is.na(db)) == 0,]
  title_str <- paste('Top mentions by people tweeting ',constituent,sep='')
  
  ggplot(db,aes(x=reorder(db$trend,db$count),y=db$count))+geom_col(fill='#3A87C8')+
    labs(y="Count",x="Words")+
    ggtitle(title_str)+coord_flip()+
    scale_fill_brewer(direction = -1)+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
}

#This plots the average twitter sentiment by day. 
avg_twitter_sent<-function(data_sent,constituent){
  
  data_sent_all<-data_sent_all[,c('date','constituent','avg_sentiment')]
  ##select the date wanted
  data_sent<-data_sent_all
  data_sent$date<-as.character(data_sent$date)
  data_sent$date<-as.Date(data_sent$date,"%Y-%m-%d")
  data_sent<-data_sent[data_sent$date>as.Date('2017-10-06'),]
  
  if(constituent=='Adidas'){
    sent_df<- data_sent[data_sent$constituent==tolower(constituent),]}else{
      sent_df <- data_sent[data_sent$constituent==constituent,]}
  
  
  df<-sent_df[,c('date','avg_sentiment')]
  sent_df_clean<-df[rowSums(is.na(df)) == 0,]
  
  title_str = paste('Daily Twitter Sentiment for ',constituent, sep='')
  ggplot(data=sent_df_clean, aes(x=sent_df_clean$date, y=sent_df_clean$avg_sentiment,group=1))+geom_line()+
    geom_point()+
    labs(y="Average Twitter Sentiment",x="Date")+
    labs(title = title_str)+
    ylim(-1,1)+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
}

##This function creates a color-coded world map according to Twitter Sentiment
##Find the minimum/maximum of the sentiment, in order to decide the range of colors.
map_sentiment<-function(country_df,constituent){
  title_str = paste('Twitter sentiment for ',constituent,' by countries',sep='')
  if(constituent=='Adidas'){
    df<-country_df[country_df$constituent==tolower(constituent),c('count','avg_sentiment','country')]}else{
      df<-country_df[country_df$constituent==constituent,c('count','avg_sentiment','country')]}
  min_sent<-min(df$avg_sentiment, na.rm=T)
  max_sent<-max(df$avg_sentiment, na.rm=T)
  if ((min_sent<=0)&(max_sent>=0)){palette_color = c('red','#EB984E','#FFCC00','#1E8449')}
  if ((min_sent<=0)&(max_sent<=0)){palette_color = c('red','#EB984E','#FFCC00')}
  if ((min_sent>=0)&(max_sent>=0)){palette_color = c('#FFCC00','#1E8449')}

  n <- joinCountryData2Map(df, joinCode="ISO2", nameJoinColumn="country")
  mapCountryData(n, nameColumnToPlot="avg_sentiment", mapTitle=title_str,colourPalette=rwmGetColours(palette_color,length(palette_color)))
}


##This function creates a color-coded world map according to Tweet Frequency
map_frequency<-function(country_df,constituent){
  title_str = paste('Tweeting frequency for ',constituent,' by countries',sep='')
  if (constituent=='Adidas'){
    df<-country_df[country_df$constituent==tolower(constituent),c('count','avg_sentiment','country')]}else{
      df<-country_df[country_df$constituent==constituent,c('count','avg_sentiment','country')]}
  
  n <- joinCountryData2Map(df, joinCode="ISO2", nameJoinColumn="country")
  mapCountryData(n, nameColumnToPlot="count", mapTitle=title_str,colourPalette=rwmGetColours(c('#F5EEF8','#D7BDE2','#9B59B6','#7D3C98'),4))
}


