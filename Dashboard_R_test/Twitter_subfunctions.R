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
      price<- df2[df2$constituent==constituent,c('name','value')]}
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
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=1.0, face="bold"))
}

#This plots the average twitter sentiment by day. 
avg_twitter_sent<-function(data_sent_all,constituent){
  
  data_sent_all<-data_sent_all[,c('date','constituent','avg_sentiment')]
  ##select the date wanted
  data_sent<-data_sent_all
  data_sent$date<-as.character(data_sent$date)
  data_sent$date<-as.Date(data_sent$date,"%Y-%m-%d")
  data_sent<-data_sent[data_sent$date>as.Date('2017-11-01'),]
  
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
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=1.0, face="bold"))
}

##This function creates a color-coded world map according to Twitter Sentiment
##Find the minimum/maximum of the sentiment, in order to decide the range of colors.
map_sentiment<-function(country_df,constituent){
  #title_str = paste('Twitter sentiment for ',constituent,' by countries',sep='')
  df<-country_df[country_df$constituent_name==constituent_names_array[[constituent]],c('count','avg_sentiment','country')]
  min_sent<-min(df$avg_sentiment, na.rm=T)
  max_sent<-max(df$avg_sentiment, na.rm=T)
  if ((min_sent<=0)&(max_sent>=0)){palette_color = c('red','#EB984E','#FFCC00','#1E8449')}
  if ((min_sent<=0)&(max_sent<=0)){palette_color = c('red','#EB984E','#FFCC00')}
  if ((min_sent>=0)&(max_sent>=0)){palette_color = c('#FFCC00','#1E8449')}
  
  n <- joinCountryData2Map(df, joinCode="ISO2", nameJoinColumn="country")
  par(mar=c(0,0,0,0))
  mapCountryData(n, nameColumnToPlot="avg_sentiment", mapTitle='',colourPalette=rwmGetColours(palette_color,length(palette_color)))
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
  if (constituent=='Adidas'){
    df<-country_df[country_df$constituent==tolower(constituent),c('count','avg_sentiment','country')]}else{
      df<-country_df[country_df$constituent==constituent,c('count','avg_sentiment','country')]}
  
  n <- joinCountryData2Map(df, joinCode="ISO2", nameJoinColumn="country")
  par(mar=c(0,0,0,0))
  mapCountryData(n, nameColumnToPlot="count", mapTitle='',colourPalette=rwmGetColours(c('#F5EEF8','#D7BDE2','#9B59B6','#7D3C98'),4))
}
