##Stores all the functions used for the Homepage of Analytics Dashboard. 

#This function counts the number of positive, negative and neutral tweets for one specific stock. 
tweet_count<-function(constituent_count){
  
  pos_line = constituent_count[constituent_count$line=='Positive',c("count")]
  neg_line = constituent_count[constituent_count$line=='Negative',c("count")]
  neu_line = constituent_count[constituent_count$line=='Neutral',c("count")]
  constituent = constituent_count$constituent[1]
  title_str = paste('Number of Tweets for ',constituent,sep='')
  
  if (constituent =='EON'){
  neg_line <- append(neg_line,0,after=min(neg_line))}
  
  date_range <- constituent_count[constituent_count$line=='Neutral',c("date")]
  date_range <- as.Date(date_range,"%Y-%m-%d")
  date_range<-gsub("-", "/", date_range)
  date_range <- as.Date(date_range,"%Y/%m/%d")
  
  constituent_count_posi<-constituent_count[constituent_count$line=='Positive',c("count",'date')]
  ggplot(data=constituent_count_posi)+
    geom_line(aes(x=date_range,y=pos_line,group=1,color="Positive")) +geom_point(aes(x=date_range,y=pos_line,group=1,color="Positive"))+
    geom_line(aes(x=date_range,y=neg_line,group=1,color="Negative")) +geom_point(aes(x=date_range,y=neg_line,group=1,color="Negative"))+
    geom_line(aes(x=date_range,y=neu_line,group=1,color="Neutral")) +geom_point(aes(x=date_range,y=neu_line,group=1,color="Neutral"))+
    ggtitle(title_str)+labs(y="Number of Tweets",x="Date")+
    scale_colour_manual(name="Sentiment",values=c("Positive"="#1E8449","Neutral"="#FFCC00","Negative"="red"))+
    theme(legend.position="bottom",legend.direction='horizontal')+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
}






## This is a function that only extract the first n words of a string object. 
string_fun <- function(x) {
  ul = unlist(strsplit(x, split = "\\s+"))[1:8] #set n=8
  paste(ul,collapse=" ")
}


##This function transforms the all_news database into a presentable DataTable
news_transform<-function(db){
  db$NEWS_DATE_NewsDim<- as.Date(db$NEWS_DATE_NewsDim,format='%d/%m/%Y')
  db<- db[order(-as.numeric(db$NEWS_DATE_NewsDim)),] ##order by release dates, descending
  db <- db[1:7398,c('NEWS_TITLE_NewsDim','constituent','categorised_tag','sentiment')]

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
  db[!is.na(db$constituent) & db$constituent=='bmw', c('constituent')] ='BMW'
  #db[db$constituent=='adidas',c('constituent')]='Adidas'
  db[!is.na(db$constituent) & db$constituent=='adidas', c('constituent')] ='Adidas'
  #db[db$constituent=='commerzbank',c('constituent')]='Commerzbank'
  db[!is.na(db$constituent) & db$constituent=='commerzbank', c('constituent')] ='Commerzbank'
  #db[db$constituent=='eon',c('constituent')]='EON'
  db[!is.na(db$constituent) & db$constituent=='eon', c('constituent')] ='EON'

#db$Newslink<-paste('<a href="',db$Link,'">',db$Headline ,'</a>',sep="") #embed the hyperlink in headlines

  df<- datatable(db[,c('NEWS_TITLE_NewsDim','constituent','sentiment')],rownames=FALSE, options = list(pageLength = 5),colnames = c('Headline' ,'Constituent','Sentiment'),escape=FALSE) %>%
    formatStyle('sentiment',
              color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
              backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))
}

#This function creates a stacked bar for analyst recommendations. 
analyst_stacked_bar<-function(retrieved_data){
  df<- retrieved_data[,c('Constituent','% Buy','% Hold','% Sell')]
  v<-(sort(df$Constituent,decreasing=FALSE))
  df<-df[match(v,df$Constituent),]
  dat.m <- melt(df,id.vars = "Constituent") 
  dat.m$Percentage = dat.m$value*100
  names(dat.m)[names(dat.m) == 'variable'] <- 'Key'
  ggplot(dat.m,aes(x = dat.m$Constituent, y = Percentage,fill=Key,label=paste(Percentage,'%',sep='')))+
    geom_bar(stat='identity')+coord_flip()+scale_fill_manual(values = c("#1E8449",'#FFCC00','red'))+
    geom_text(size = 3, position = position_stack(vjust = 0.5))+ggtitle('Analyst Recommendations' )+
    theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))+
    theme(legend.position="bottom",legend.direction='horizontal')+theme(plot.margin = unit(c(2,0,0,0), "cm"))
}


##This function takes the table from Mongodb and turns it into a color-coded summary datatable
summary_box<-function(retrieved_data){
  df<-retrieved_data[,c('constituent','twitter_sentiment','news_rating','profitability','risk')]
  datatable(df,options=list(dom='t'),rownames = FALSE,colnames = c('Twitter Sentiment', 'News Rating','Profitability', 'Risk')) %>%
  formatStyle(c('profitability','risk','twitter_sentiment','news_rating'),
              color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
              backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))}
