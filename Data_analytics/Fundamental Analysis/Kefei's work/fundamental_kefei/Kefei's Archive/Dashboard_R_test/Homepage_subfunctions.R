##Stores all the functions used for the Homepage of Analytics Dashboard. 
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
library(treemap)

popular_tweet_treemap<-function(df){
  df[df['constituent']=='adidas',c('constituent')]='Adidas'
  df$label <- paste(df$constituent,',', ' ' ,df$count, ' tweets' ,sep = "")
  
  treemap(df,index="label",vSize="count",
          vColor = "avg_sentiment_all",
          type='manual',
          palette="RdYlGn", 
          algorithm = 'squarified',
          mapping=c(-0.15, 0, 0.15),
          fontsize.labels=c(15,12),
          title="Most Tweeted Consistuent vs. Sentiment - Last 2 Months",
          fontsize.title = c(16),
          fontface.labels=c(1,1),
          #bg.labels=c("transparent"), 
          border.col=c("white","white"), 
          border.lwds=c(7,2),
          align.labels=list(
            c("center", "center"), 
            c("right", "bottom")
          ),      
          title.legend="Average sentiment",
          overlap.labels=0.5,
          inflate.labels=F
  )}


##This function plots a barchart for the most tweeted constituents
popular_constituents_bar<-function(top_tweeted_constituents){
  title_str = 'Most tweeted DAX constituents in the last 2 months'
  df<-top_tweeted_constituents[,c('constituent','count')]
  df<-df[df$constituent!='DAX',]
  df[df$constituent=='adidas',c('constituent')]='Adidas'
  ggplot(df,aes(x= reorder(constituent, count),y=df$count))+geom_col(fill='#3A87C8')+labs(y="Number of tweets",x="Constituents") +
    coord_flip()+
    ggtitle(title_str) +scale_fill_discrete(guide=FALSE)+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
}



## This is a function that only extract the first n words of a string object. 
string_fun <- function(x) {
  ul = unlist(strsplit(x, split = "\\s+"))[1:10] #set n=8
  paste(ul,collapse=" ")
}


##This function transforms the all_news database into a presentable DataTable
news_transform<-function(db){
  db$NEWS_DATE_NewsDim<- as.Date(db$NEWS_DATE_NewsDim,format='%d/%m/%Y')
  db<- db[order(-as.numeric(db$NEWS_DATE_NewsDim)),] ##order by release dates, descending
  db <- db[1:30,c('NEWS_TITLE_NewsDim','constituent','categorised_tag','sentiment')]
  
  #make sure the news link only contains 8 characters from the headline. 
  db$NEWS_TITLE_NewsDim <- as.character(db$NEWS_TITLE_NewsDim)
  db$NEWS_TITLE_NewsDim <- unlist(lapply(db$NEWS_TITLE_NewsDim, string_fun)) ##Apply the limit
  ##remove the NA
  db$NEWS_TITLE_NewsDim <-gsub("NA", " ", db$NEWS_TITLE_NewsDim)
  
  
  ##Assign the conditional sentiment values for conditioned-colors
  index_posi<-db$sentiment=='positive'
  db$sentiment[index_posi]<-1
  index_nega<-db$sentiment=='negative'
  db$sentiment[index_nega]<-1
  index_neu<-db$sentiment=='neutral' 
  db$sentiment[index_neu]=0
  
  ##Fix the capital letters
  #db[db$constituent=='bmw',c('constituent')]='BMW'
  #db[!is.na(db$constituent) & db$constituent=='bmw', c('constituent')] ='BMW'
  
  #db$Newslink<-paste('<a href="',db$Link,'">',db$Headline ,'</a>',sep="") #embed the hyperlink in headlines
  
  df<- datatable(db[,c('NEWS_TITLE_NewsDim','constituent','sentiment')],rownames=FALSE, options = list(pageLength = 5),colnames = c('Headline' ,'Constituent','Sentiment'),escape=FALSE) %>%
    formatStyle('sentiment',
                color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
                backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))
}


#This function creates a stacked bar for analyst recommendations, A-D. 
analyst_stacked_bar_1<-function(retrieved_data){
  df<- retrieved_data[,c('Constituent','% Buy','% Hold','% Sell')]
  df[df$Constituent =='Münchener Rückversicherungs-Gesellschaft',c('Constituent')] ='Münchener RG'
  df<-df[order(df$Constituent),] #Rank by the most. 
  df<-df[1:10,]
  
  dat.m <- melt(df,id.vars = "Constituent") 
  dat.m$Percentage = dat.m$value*100
  names(dat.m)[names(dat.m) == 'variable'] <- 'Key'
  ggplot(dat.m[order(dat.m$Constituent, decreasing = T),],aes(x = Constituent, y = Percentage,fill=Key,label=paste(Percentage,'%',sep='')))+
    geom_bar(stat='identity')+coord_flip()+scale_fill_manual(values = c("#1E8449",'#FFCC00','red'))+
    geom_text(size = 3, position = position_stack(vjust = 0.5))+ggtitle('' )+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))+
    theme(legend.position="bottom",legend.direction='horizontal')+theme(plot.margin = unit(c(1,0,0,0), "cm"))
}


analyst_stacked_bar_2<-function(retrieved_data){
  df<- retrieved_data[,c('Constituent','% Buy','% Hold','% Sell')]
  df[df$Constituent =='Münchener Rückversicherungs-Gesellschaft',c('Constituent')] ='Münchener RG'
  df<-df[order(df$Constituent),] #Rank by the most. 
  df<-df[11:20,]
  
  dat.m <- melt(df,id.vars = "Constituent") 
  dat.m$Percentage = dat.m$value*100
  names(dat.m)[names(dat.m) == 'variable'] <- 'Key'
  ggplot(dat.m[order(dat.m$Constituent, decreasing = T),],aes(x = Constituent, y = Percentage,fill=Key,label=paste(Percentage,'%',sep='')))+
    geom_bar(stat='identity')+coord_flip()+scale_fill_manual(values = c("#1E8449",'#FFCC00','red'))+
    geom_text(size = 3, position = position_stack(vjust = 0.5))+ggtitle('' )+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))+
    theme(legend.position="bottom",legend.direction='horizontal')+theme(plot.margin = unit(c(1,0,0,0), "cm"))
}

analyst_stacked_bar_3<-function(retrieved_data){
  df<- retrieved_data[,c('Constituent','% Buy','% Hold','% Sell')]
  df[df$Constituent =='Münchener Rückversicherungs-Gesellschaft',c('Constituent')] ='Münchener RG'
  df<-df[order(df$Constituent),] #Rank by the most. 
  df<-df[21:nrow(df),]
  
  dat.m <- melt(df,id.vars = "Constituent") 
  dat.m$Percentage = dat.m$value*100
  names(dat.m)[names(dat.m) == 'variable'] <- 'Key'
  ggplot(dat.m[order(dat.m$Constituent, decreasing = T),],aes(x = Constituent, y = Percentage,fill=Key,label=paste(Percentage,'%',sep='')))+
    geom_bar(stat='identity')+coord_flip()+scale_fill_manual(values = c("#1E8449",'#FFCC00','red'))+
    geom_text(size = 3, position = position_stack(vjust = 0.5))+ggtitle('' )+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))+
    theme(legend.position="bottom",legend.direction='horizontal')+theme(plot.margin = unit(c(1,0,0,0), "cm"))
}




##This function takes the table from Mongodb and turns it into a color-coded summary datatable
summary_box_1<-function(retrieved_data){
  df<-retrieved_data[,c('constituent','twitter_sentiment','news_sentiment','profitability','risk')]
  df<-df[order(df$constituent),]
  df<-df[1:10,]
  datatable(df,options=list(dom='t'),rownames = FALSE,colnames = c('Twitter Sentiment', 'News Sentiment','Profitability', 'Risk')) %>%
  formatStyle(c('profitability','risk','twitter_sentiment','news_sentiment'),
              color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
              backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))}

summary_box_2<-function(retrieved_data){
  df<-retrieved_data[,c('constituent','twitter_sentiment','news_sentiment','profitability','risk')]
  df<-df[order(df$constituent),]
  df<-df[11:20,]
  datatable(df,options=list(dom='t'),rownames = FALSE,colnames = c('Twitter Sentiment', 'News Sentiment','Profitability', 'Risk')) %>%
    formatStyle(c('profitability','risk','twitter_sentiment','news_sentiment'),
                color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
                backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))}

summary_box_3<-function(retrieved_data){
  df<-retrieved_data[,c('constituent','twitter_sentiment','news_sentiment','profitability','risk')]
  df<-df[order(df$constituent),]
  df<-df[21:nrow(df),]
  datatable(df,options=list(dom='t'),rownames = FALSE,colnames = c('Twitter Sentiment', 'News Sentiment','Profitability', 'Risk')) %>%
    formatStyle(c('profitability','risk','twitter_sentiment','news_sentiment'),
                color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
                backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))}

