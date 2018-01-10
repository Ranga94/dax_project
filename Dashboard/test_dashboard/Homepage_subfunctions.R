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


constituent_array = c('Merck','Daimler','Bayer','Allianz','Adidas','BMW','Commerzbank','Deutsche Bank','EON',
                      'SAP','Continental','Siemens','Lufthansa','BASF','Thyssenkrupp','Infineon', 'Linde',
                      'RWE','Deutsche Telekom','Fresenius Medical Care','Fresenius','Deutsche Post','Deutsche Börse',
                      'Beiersdorf','Vonovia','Volkswagen','ProSiebenSat1 Media','HeidelbergCement','Henkel')

constituent_names_array<-c('MERCK KGAA','DAIMLER AG','BAYER AG','ALLIANZ SE','ADIDAS AG','BAYERISCHE MOTOREN WERKE AG','COMMERZBANK AKTIENGESELLSCHAFT','DEUTSCHE BANK AG',
                           'E.ON SE','SAP SE','CONTINENTAL AG','SIEMENS AG','DEUTSCHE LUFTHANSA AG','BASF SE','THYSSENKRUPP AG',
                           'INFINEON TECHNOLOGIES AG','LINDE AG','RWE AG','DEUTSCHE TELEKOM AG','FRESENIUS MEDICAL CARE AG & CO.KGAA',
                           'FRESENIUS SE & CO.KGAA','DEUTSCHE POST AG','DEUTSCHE BOERSE AG','BEIERSDORF AG',
                           'VONOVIA SE','VOLKSWAGEN AG','PROSIEBENSAT.1 MEDIA SE','HEIDELBERGCEMENT AG','HENKEL AG & CO. KGAA')


##This dictionary translates constituent_name into constituent
names(constituent_array)<-constituent_names_array 

popular_tweet_treemap<-function(df){
  df[df$constituent=='adidas',c('constituent')]='Adidas'
  df$label <- paste(df$constituent,',', ' ' ,df$count ,sep = "")
  
  treemap(df,index="label",vSize="count",
          #vColor = "avg_sentiment_all",
          vColor = "count",
          type='manual',
          #palette="RdYlGn", 
          palette = '#B865B4',
          algorithm = 'squarified',
          mapping=c(-0.25, 0, 0.25),
          #mapping=c(-1, 0, 1),
          fontsize.labels=c(10,10),
          title="Most Tweeted Consistuent Count vs. Sentiment - Last 2 Months",
          fontsize.title = c(14),
          fontface.labels=c(0,0), #Trying to hide the legend. 
          #bg.labels=c("transparent"),
          fontsize.legend = 0, #hide legend
          border.col=c("white","white"), 
          border.lwds=c(7,2),
          align.labels=list(
            c("center", "center"), 
            c("right", "bottom")
          ),      
          #title.legend="Average sentiment",
          overlap.labels=0.5,
          inflate.labels=F
  )
  }


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
  ul = unlist(strsplit(x, split = "\\s+"))[1:8] #set n=8
  paste(ul,collapse=" ")
}


##This function transforms the all_news database into a presentable DataTable
news_transform<-function(db){
  #db$NEWS_DATE_NewsDim<- as.Date(db$NEWS_DATE_NewsDim,format='%d/%m/%Y')
  #db<- db[order(-as.numeric(db$NEWS_DATE_NewsDim)),] ##order by release dates, descending
  
  #db <- db[1:30,]
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
  #db$Newslink<-paste('<a href="',db$Link,'">',db$Headline ,'</a>',sep="") #embed the hyperlink in headlines
  
  db[db['constituent']=='adidas',c('constituent')]<-'Adidas'
  df<- datatable(db[order(db$constituent),c('NEWS_TITLE_NewsDim','constituent','sentiment')],rownames=FALSE, options = list(pageLength = 5),colnames = c('Headline' ,'Constituent','Sentiment'),escape=FALSE) %>%
    formatStyle('sentiment',
                color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
                backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))
}


#This function creates a stacked bar for analyst recommendations, A-D. 
analyst_stacked_bar_1<-function(retrieved_data){
  df<- retrieved_data[,c('Constituent','Buy_percentage','Hold_percentage','Sell_percentage')]
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
  df<- retrieved_data[,c('Constituent','Buy_percentage','Hold_percentage','Sell_percentage')]
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
  df<- retrieved_data[,c('Constituent','Buy_percentage','Hold_percentage','Sell_percentage')]
  df[df$Constituent =='Münchener Rückversicherungs-Gesellschaft',c('Constituent')] ='Münchener RG'
  df[df$Constituent=='thyssenkrupp',c('Constituent')] = 'Thyssenkrupp'
  df[df$Constituent=='Volkswagen (VW) vz',c('Constituent')] = 'Volkswagen'
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
  df<-retrieved_data[,c('Constituent','Twitter_sent_color','News_sent_color','Profitability_color','Risk_color')]
  df<-df[1:10,]
  df[df$Constituent=='adidas',c('Constituent')] = 'Adidas'
  datatable(df,options=list(dom='t'),rownames = FALSE,colnames = c('Twitter Sentiment', 'News Sentiment','Profitability', 'Risk')) %>%
    formatStyle(c('Twitter_sent_color','News_sent_color','Profitability_color','Risk_color'),
                color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
                backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))}


summary_box_2<-function(retrieved_data){
  df<-retrieved_data[,c('Constituent','Twitter_sent_color','News_sent_color','Profitability_color','Risk_color')]
  df<-df[11:20,]
  #df<-df[df$constituent!='Henkel vz',]
  #df<-df[df$constituent!='Linde',]
  datatable(df,options=list(dom='t'),rownames = FALSE,colnames = c('Twitter Sentiment', 'News Sentiment','Profitability', 'Risk')) %>%
    formatStyle(c('Twitter_sent_color','News_sent_color','Profitability_color','Risk_color'),
                color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
                backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))}

summary_box_3<-function(retrieved_data){
  df<-retrieved_data[,c('Constituent','Twitter_sent_color','News_sent_color','Profitability_color','Risk_color')]
  #df<-df[df$constituent!='Volkswagen (VW) vz',]
  #df<-df[df$constituent!='Münchener Rückversicherungs-Gesellschaft',]
  #df[df$constituent == 'Münchener Rückversicherungs-Gesellschaft',c('constituent')]='Münchener RG'
  df<-df[21:nrow(df),]
  df[df$Constituent=='thyssenkrupp',c('Constituent')] = 'Thyssenkrupp'
  df[df$Constituent=='Volkswagen (VW) vz',c('Constituent')] = 'Volkswagen'
  df[df$Constituent =='Münchener Rückversicherungs-Gesellschaft',c('Constituent')] ='Münchener RG'
  datatable(df,options=list(dom='t'),rownames = FALSE,colnames = c('Twitter Sentiment', 'News Sentiment','Profitability', 'Risk')) %>%
    formatStyle(c('Twitter_sent_color','News_sent_color','Profitability_color','Risk_color'),
                color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
                backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))}
