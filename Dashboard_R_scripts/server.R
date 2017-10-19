library(DT)
library(shiny)
library(shinydashboard)

##Create a function that color codes table
##COLOR-CODED TABLE
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

server <- function(input, output){
  
######################################  Main content of the dashboard  ##############################################
  
  ##############################    HOMEPAGE    ###################################
  ## The Twitter Sentiment Count - Line plot
  twitter_count <- mongo(collection = 'twitter_sentiment_count_daily',
                         url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                         verbose = FALSE, options = ssl_options())
  twitter_df<-twitter_count$find('{}')
  
  ##BMW
  tweet_count_bmw <-eventReactive(input$reload, {
    constituent_count <- twitter_df[twitter_df$constituent=='BMW',]
    pos_line = constituent_count[constituent_count$line=='Positive',c("count")]
    neg_line = constituent_count[constituent_count$line=='Negative',c("count")]
    neu_line = constituent_count[constituent_count$line=='Neutral',c("count")]
    date_range <- constituent_count[constituent_count$line=='Neutral',c("date")]
    date_range <- as.Date(date_range,"%Y-%m-%d")
    date_range<-gsub("-", "/", date_range)
    date_range <- as.Date(date_range,"%Y/%m/%d")
    constituent_count_posi<-constituent_count[constituent_count$line=='Positive',c("count",'date')]
    ggplot(data=constituent_count_posi)+
      geom_line(aes(x=date_range,y=pos_line,group=1,color="Positive")) +geom_point(aes(x=date_range,y=pos_line,group=1,color="Positive"))+
      geom_line(aes(x=date_range,y=neg_line,group=1,color="Negative")) +geom_point(aes(x=date_range,y=neg_line,group=1,color="Negative"))+
      geom_line(aes(x=date_range,y=neu_line,group=1,color="Neutral")) +geom_point(aes(x=date_range,y=neu_line,group=1,color="Neutral"))+
      ggtitle('Number of Tweets for BMW')+labs(y="Number of Tweets",x="Date")+
      scale_colour_manual(name="Sentiment",values=c("Positive"="#1E8449","Neutral"="#FFCC00","Negative"="red"))+
      theme(legend.position="bottom",legend.direction='horizontal')+
      theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
    
   
  },ignoreNULL = FALSE)
  output$tweet_num_bmw<-renderPlot(tweet_count_bmw())
  
  ## Adidas
  tweet_count_adidas <-eventReactive(input$reload, {
    constituent_count <- twitter_df[twitter_df$constituent=='adidas',]
    pos_line = constituent_count[constituent_count$line=='Positive',c("count")]
    neg_line = constituent_count[constituent_count$line=='Negative',c("count")]
    neu_line = constituent_count[constituent_count$line=='Neutral',c("count")]
    date_range <- constituent_count[constituent_count$line=='Neutral',c("date")]
    date_range <- as.Date(date_range,"%Y-%m-%d")
    date_range<-gsub("-", "/", date_range)
    date_range <- as.Date(date_range,"%Y/%m/%d")
    constituent_count_posi<-constituent_count[constituent_count$line=='Positive',c("count",'date')]
    ggplot(data=constituent_count_posi)+
      geom_line(aes(x=date_range,y=pos_line,group=1,color="Positive")) +geom_point(aes(x=date_range,y=pos_line,group=1,color="Positive"))+
      geom_line(aes(x=date_range,y=neg_line,group=1,color="Negative")) +geom_point(aes(x=date_range,y=neg_line,group=1,color="Negative"))+
      geom_line(aes(x=date_range,y=neu_line,group=1,color="Neutral")) +geom_point(aes(x=date_range,y=neu_line,group=1,color="Neutral"))+
      ggtitle('Number of Tweets for Adidas')+labs(y="Number of Tweets",x="Date")+
      scale_colour_manual(name="Sentiment",values=c("Positive"="#1E8449","Neutral"="#FFCC00","Negative"="red"))+
      theme(legend.position="bottom",legend.direction='horizontal')+
      theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
    
  },ignoreNULL = FALSE)
  output$tweet_num_adidas<-renderPlot(tweet_count_adidas())
  
  
  ## Commerzbank
  tweet_count_cb <-eventReactive(input$reload, {
    constituent_count <- twitter_df[twitter_df$constituent=='Commerzbank',]
    pos_line = constituent_count[constituent_count$line=='Positive',c("count")]
    neg_line = constituent_count[constituent_count$line=='Negative',c("count")]
    neu_line = constituent_count[constituent_count$line=='Neutral',c("count")]
    date_range <- constituent_count[constituent_count$line=='Neutral',c("date")]
    date_range <- as.Date(date_range,"%Y-%m-%d")
    date_range<-gsub("-", "/", date_range)
    date_range <- as.Date(date_range,"%Y/%m/%d")
    constituent_count_posi<-constituent_count[constituent_count$line=='Positive',c("count",'date')]
    ggplot(data=constituent_count_posi)+
      geom_line(aes(x=date_range,y=pos_line,group=1,color="Positive")) +geom_point(aes(x=date_range,y=pos_line,group=1,color="Positive"))+
      geom_line(aes(x=date_range,y=neg_line,group=1,color="Negative")) +geom_point(aes(x=date_range,y=neg_line,group=1,color="Negative"))+
      geom_line(aes(x=date_range,y=neu_line,group=1,color="Neutral")) +geom_point(aes(x=date_range,y=neu_line,group=1,color="Neutral"))+
      ggtitle('Number of Tweets for Commerzbank')+labs(y="Number of Tweets",x="Date")+
      scale_colour_manual(name="Sentiment",values=c("Positive"="#1E8449","Neutral"="#FFCC00","Negative"="red"))+
      theme(legend.position="bottom",legend.direction='horizontal')+
      theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
  },ignoreNULL = FALSE)
  output$tweet_num_cb<-renderPlot(tweet_count_cb())
  
  
  ## Deutsche Bank
  tweet_count_db <-eventReactive(input$reload, {
    constituent_count <- twitter_df[twitter_df$constituent=='Deutsche Bank',]
    pos_line = constituent_count[constituent_count$line=='Positive',c("count")]
    neg_line = constituent_count[constituent_count$line=='Negative',c("count")]
    neu_line = constituent_count[constituent_count$line=='Neutral',c("count")]
    date_range <- constituent_count[constituent_count$line=='Neutral',c("date")]
    date_range <- as.Date(date_range,"%Y-%m-%d")
    date_range<-gsub("-", "/", date_range)
    date_range <- as.Date(date_range,"%Y/%m/%d")
    constituent_count_posi<-constituent_count[constituent_count$line=='Positive',c("count",'date')]
    ggplot(data=constituent_count_posi)+
      geom_line(aes(x=date_range,y=pos_line,group=1,color="Positive")) +geom_point(aes(x=date_range,y=pos_line,group=1,color="Positive"))+
      geom_line(aes(x=date_range,y=neg_line,group=1,color="Negative")) +geom_point(aes(x=date_range,y=neg_line,group=1,color="Negative"))+
      geom_line(aes(x=date_range,y=neu_line,group=1,color="Neutral")) +geom_point(aes(x=date_range,y=neu_line,group=1,color="Neutral"))+
      ggtitle('Number of Tweets for Deutsche Bank')+labs(y="Number of Tweets",x="Date")+
      scale_colour_manual(name="Sentiment",values=c("Positive"="#1E8449","Neutral"="#FFCC00","Negative"="red"))+
      theme(legend.position="bottom",legend.direction='horizontal')+
      theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
    
  },ignoreNULL = FALSE)
  output$tweet_num_db<-renderPlot(tweet_count_db())
  
  
  ## EON
  tweet_count_eon <-eventReactive(input$reload, {
    constituent_count <- twitter_df[twitter_df$constituent=='EON',]
    pos_line = constituent_count[constituent_count$line=='Positive',c("count")]
    neg_line = constituent_count[constituent_count$line=='Negative',c("count")]
    neu_line = constituent_count[constituent_count$line=='Neutral',c("count")]
    ##Normalize the same size of vectors 
    neg_line <- append(neg_line,0,after=min(neg_line))
    
    date_range <- constituent_count[constituent_count$line=='Neutral',c("date")]
    date_range <- as.Date(date_range,"%Y-%m-%d")
    date_range<-gsub("-", "/", date_range)
    date_range <- as.Date(date_range,"%Y/%m/%d")
    
    constituent_count_posi<-constituent_count[constituent_count$line=='Positive',c("count",'date')]
    ggplot(data=constituent_count_posi)+
      geom_line(aes(x=date_range,y=pos_line,group=1,color="Positive")) +geom_point(aes(x=date_range,y=pos_line,group=1,color="Positive"))+
      geom_line(aes(x=date_range,y=neg_line,group=1,color="Negative")) +geom_point(aes(x=date_range,y=neg_line,group=1,color="Negative"))+
      geom_line(aes(x=date_range,y=neu_line,group=1,color="Neutral")) +geom_point(aes(x=date_range,y=neu_line,group=1,color="Neutral"))+
      ggtitle('Number of Tweets for EON')+labs(y="Number of Tweets",x="Date")+
      scale_colour_manual(name="Sentiment",values=c("Positive"="#1E8449","Neutral"="#FFCC00","Negative"="red"))+
      theme(legend.position="bottom",legend.direction='horizontal')+
      theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
    
  },ignoreNULL = FALSE)
  output$tweet_num_eon<-renderPlot(tweet_count_eon())
  
  
  
  ## News Table - DataTable
  ## This is a function that only extract the first n words of a string object. 
  string_fun <- function(x) {
      ul = unlist(strsplit(x, split = "\\s+"))[1:8] #set n=8
    paste(ul,collapse=" ")
  }
  
  
  news_data_all <- eventReactive(input$reload, {
    all_news <- mongo(collection = 'all_news',
                url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                verbose = FALSE, options = ssl_options())
    db <- all_news$find('{"show":true}')
    db$NEWS_DATE_NewsDim<- as.Date(db$NEWS_DATE_NewsDim,format='%d/%m/%Y')
    db<- db[order(-as.numeric(db$NEWS_DATE_NewsDim)),] ##order by release dates, descending
   
    db <- db[1:100,c('NEWS_TITLE_NewsDim','constituent','categorised_tag','sentiment')]
   
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
    db[db$constituent=='bmw',c('constituent')]='BMW'
    db[db$constituent=='adidas',c('constituent')]='Adidas'
    db[db$constituent=='commerzbank',c('constituent')]='Commerzbank'
    db[db$constituent=='eon',c('constituent')]='EON'
    
    #db$Newslink<-paste('<a href="',db$Link,'">',db$Headline ,'</a>',sep="") #embed the hyperlink in headlines
    
    df<- datatable(db[,c('NEWS_TITLE_NewsDim','constituent','sentiment')],rownames=FALSE, options = list(pageLength = 5),colnames = c('Headline' ,'Constituent','Sentiment'),escape=FALSE) %>%
      formatStyle('sentiment',
                  color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
                  backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))
  }, ignoreNULL = FALSE)
  output$news_all <- DT::renderDataTable(news_data_all())
  
    
  
  ## Analyst Recommendation Percentage - Stacked Bar Chart
  analyst_data <- eventReactive(input$reload, {
    db <- mongo(collection = 'analyst_opinions',
                url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find('{"status":"active"}')
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
  }, ignoreNULL = FALSE)
  output$analystplot <- renderPlot(analyst_data())

  
  ##Summary Box - DataTable
  summary_data <- eventReactive(input$reload, {
    db <- mongo(collection = 'summary_box',
                url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find()
    df<-retrieved_data[,c('constituent','twitter_sentiment','news_rating','profitability','risk')]
    datatable(df,options=list(dom='t'),rownames = FALSE,colnames = c('Twitter Sentiment', 'News Rating','Profitability', 'Risk')) %>%
      formatStyle(c('profitability','risk','twitter_sentiment','news_rating'),
                  color = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')),
                  backgroundColor = styleInterval(c(-1,0),c('red','#FFCC00','#1E8449')))
                  
  }, ignoreNULL = FALSE)
  output$reactivetable <- renderDataTable(summary_data())
  
  
  ################################# FUNDAMENTAL ###########################################
  ##Cumulative Returns - DataTable
  cumulative_return <- eventReactive(input$reload, {
    db <- mongo(collection = 'price analysis',
                url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find('{"Table":"cumulative return analysis"}')
    df<-retrieved_data[,c('Constituent','6 months return','1 year return','3 years return')]
    df<-df[order(df$Constituent),]
    df[df$Constituent=='adidas',c('Constituent')]<-'Adidas'
    df[,c('6 months return','1 year return','3 years return')]<-round(df[,c('6 months return','1 year return','3 years return')],2)
    datatable(df,rownames = FALSE,options = list(pageLength = 10),colnames = c('Constituent','6 months cml.return','1 year cml.return','3 years cml.return')) %>%
      formatStyle(c('6 months return','1 year return','3 years return'),
                  backgroundColor = styleInterval(c(0,0.5),c('red','white','#1E8449')))
  },ignoreNULL = FALSE)
  output$CRtable <- renderDataTable(cumulative_return())
  

  ##Golden Cross - DataTable
  golden_cross <- eventReactive(input$reload, {
    db <- mongo(collection = 'price analysis',
                url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find('{"Table":"Market signal"}')
    df<-retrieved_data[,c('Constituent','Recent cross','Status of SMA 50')]
    df<-df[order(df$Constituent),]
    df[df$Constituent=='adidas',c('Constituent')]<-'Adidas'
    datatable(df,rownames = FALSE,options = list(pageLength = 5),colnames = c('Constituent','Recent cross','SMA 50 movement')) 
  },ignoreNULL = FALSE)
  output$cross_table <- renderDataTable(golden_cross())
 
  
  ##Profitability Ranking - DataTable
  ranking <- eventReactive(input$reload, {
    db <- mongo(collection = 'profitability_ranking',
                url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find()
    df<-retrieved_data[,c('Profitability rank','Constituent','Price growth','Fundamental growth')]
    datatable(df,rownames = FALSE,options = list(pageLength = 10),colnames = c('Rank','Constituent','Growth in stock price','Growth in fundamental')) %>%formatStyle('Profitability rank', textAlign = 'center')
  },ignoreNULL = FALSE)
  output$ranking_top  <- renderDataTable(ranking())
  
  
  ################################# ANALYST PAGE ####################################
  ##Analyst Recommendation - DataTable
  analyst_recommendation <- eventReactive(input$reload, {
    db <- mongo(collection = 'analyst_opinions_all',
                url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find()
    df<-retrieved_data[,c('Constituent','Analyst recommendation','Analyst rating')]
    df<-df[order(df$Constituent),]
    datatable(df,rownames = FALSE,options = list(pageLength = 10), colnames = c('Constituent','Recommendation','Rating')) %>% formatStyle(
      'Analyst rating',
      background = styleColorBar(retrieved_data$`Analyst rating`, 'steelblue'),
      backgroundSize = '100% 90%',
      backgroundRepeat = 'no-repeat',
      backgroundPosition = 'center')
  },ignoreNULL = FALSE) 
  output$recommendation_table <- renderDataTable(analyst_recommendation())
  
  
  ##Analyst Target Prices - DataTable
  target_prices <- eventReactive(input$reload, {
    db <- mongo(collection = 'analyst_opinions_all',
                url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find()
    df<-retrieved_data[,c('Constituent','Median target price','Lowest target price','Highest target price')]
    df<-df[order(df$Constituent),]
    datatable(df,rownames = FALSE,options = list(pageLength = 10), colnames = c('Constituent','Median','Lowest','Highest'))  %>% formatCurrency(c('Median target price','Lowest target price','Highest target price'), '€')
  },ignoreNULL = FALSE)
  output$target_price_table <- renderDataTable(target_prices())
  
  
  ##PER Analysis - DataTable
  PER_analysis <- eventReactive(input$reload, {
    db <- mongo(collection = 'fundamental analysis',
                url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find('{"Table":"PER analysis"}')
    df<-retrieved_data[,c('Constituent','Current PER','PER last year')]
    df[df$Constituent=='adidas',c('Constituent')]<-'Adidas'
    df<-df[order(df$Constituent),]
    datatable(df,rownames = FALSE,options = list(pageLength = 5), colnames = c('Constituent','Current PER value','PER value last year'))%>% formatStyle(
      'Current PER',
      background = styleColorBar(df$`Current PER`, 'orange'),
      backgroundSize = '100% 90%',
      backgroundRepeat = 'no-repeat',
      backgroundPosition = 'center') %>%formatStyle(
      'PER last year',
      background = styleColorBar(df$`PER last year`, '#62B5F6'),
      backgroundSize = '100% 90%',
      backgroundRepeat = 'no-repeat',
      backgroundPosition = 'center')
      
    #%>%
    #formatStyle(c('Recent cross'),
    #color = styleInterval(c('Golden Cross','Death Cross'),c('red')))
  },ignoreNULL = FALSE)
  output$PER_table <- renderDataTable(PER_analysis())
  
  
  

  ##############################  TWITTER PAGE #######################################
  ##Twitter Target Price Distribution - Vertical Bar Charts 
  data_twitter <- mongo(collection = 'twitter_analytics',
              url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
              verbose = FALSE, options = ssl_options())
  df<-data_twitter$find('{"category":"price_distribution","state":"active"}')
  df2<-data_twitter$find('{"category":"influencer_distribution","state":"active"}')
  

  #General Target Prices
  output$twitter_target_price <-  renderPlot({
    
  if(input$constituent_twitter=='Adidas'){
      price<- df[df$constituent==tolower(input$constituent_twitter),c('name','value')]}else{
        price<- df[df$constituent==input$constituent_twitter,c('name','value')]}  
  
  price$name <- as.numeric(price$name)
  price$value <- as.numeric(price$value)
  mean = round(sum(price$name * price$value /100),2)
  ggplot(price,aes(x=name,y=value),main='Target Price Distribution')+geom_bar(position = "dodge",stat = "identity",fill = 'orange')+
    labs(y="% Twitter population",x="Target price (€)") +
    geom_text(aes(label=ifelse(value>=5, paste('€',name, sep=''),'')),hjust=0.5,vjust=0)
  })
  
  ##Influencer Target Prices
  output$influencer_target_price<-renderPlot({
    
  if(input$constituent_twitter=='Adidas'){
    price<- df2[df2$constituent==tolower(input$constituent_twitter),c('name','value')]}else{
  price<- df2[df2$constituent==input$constituent_twitter,c('name','value')]}
    
  price$name <- as.numeric(price$name)
  price$value <- as.numeric(price$value)
  mean = round(sum(price$name * price$value /100),2)
  ggplot(price,aes(x=name,y=value),main='Target Price Distribution')+geom_bar(position = "dodge",stat = "identity",fill = '#7D6FF0') +
    labs(y="% Twitter population",x="Target price (€)") +
    geom_text(aes(label=ifelse(value>=5, paste('€',name, sep=''),'')),hjust=0.5,vjust=0)
  })
  
  
  ##Top Mentions - Horizontal Bar Chart
  db_organization <- mongo(collection = 'twitter_top_organizations',
              url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
              verbose = FALSE, options = ssl_options())
  db_organization<-db_organization$find()
  output$organization <- renderPlot({
    if(input$constituent_twitter=='Adidas'){
      db<-db_organization[db_organization$constituent==tolower(input$constituent_twitter),c('trend','count')]}else{
      db<-db_organization[db_organization$constituent==(input$constituent_twitter),c('trend','count')]}
    
    db<-db[!(db$trend==input$constituent_twitter),]
    db<-db[!(db$trend==tolower(input$constituent_twitter)),]
    
    title_str <- paste('Top mentions by people tweeting ',input$constituent_twitter,sep='')
                       ggplot(db,aes(x=reorder(db$trend,db$count),y=db$count))+geom_col(fill='#3A87C8')+
                       labs(y="Count",x="Organizations")+
                         ggtitle(title_str)+coord_flip()+
                         scale_fill_brewer(direction = -1)+
                         theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
  })
    
  
  #Twitter Sentiment Trend - Line Plot
  data_sent <- mongo(collection = 'twitter_sentiment_trend',
                     url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                     verbose = FALSE, options = ssl_options())
  data_sent=data_sent$find('{}')
  
  output$sent_trend <-renderPlot({
    if(input$constituent_twitter=='Adidas'){
      sent_df<- data_sent[data_sent$constituent==tolower(input$constituent_twitter),]}else{
    sent_df <- data_sent[data_sent$constituent==input$constituent_twitter,]}
    df<-sent_df[,c('date','avg_sentiment')]
    
    title_str = paste('Weekly Twitter Sentiment for ',input$constituent_twitter, sep='')
    ggplot(data=df, aes(x=df$date, y=df$avg_sentiment,group=1))+geom_line()+
      geom_point()+
      labs(y="Average Twitter Sentiment",x="Date")+
      labs(title = title_str)+
      ylim(-1,1)+
      theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
  })
  
  
  ##Sentiment Pie Chart (not shown)
  db <- mongo(collection = 'twitter_analytics',
              url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
              verbose = FALSE, options = ssl_options())
  db<-db$find('{"state":"inactive","category":"sentiment"}')
  output$twitter_sentiment_pie <- renderPlot({
    if(input$constituent_twitter=='Adidas'){
      table <- db[db$constituent==tolower(input$constituent_twitter),c('name','value')]}else{
      table <- db[db$constituent==(input$constituent_twitter),c('name','value')]}
    df <- tail(table[,c('name','value')],3) ##only want the latest three entries 
    lbls = c('Postive','Neutral','Negative')
    lbls <- paste(lbls, round(df[,2])) # add percents to labels 
    lbls <- paste(lbls,"%",sep="") # ad % to labels 
    title_str<-paste('Twitter Sentiment of ',input$constituent_twitter,sep='')
    pie(df$value,labels=lbls,col=c('#1E8449','#FFCC00','red'))
  })
  
  
  
  
  ##World Twitter Data  - Map Plot
  data <- mongo(collection = 'country_data',
                url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                verbose = FALSE, options = ssl_options())
  country_df = data$find()
  
  ##Sentiment Mapping
  output$sentiment_map<-renderPlot({
    title_str = paste('Twitter sentiment for ',input$constituent_twitter,' by countries',sep='')
    if (input$constituent_twitter=='Adidas'){
      df<-country_df[country_df$constituent==tolower(input$constituent_twitter),c('count','avg_sentiment','country')]}else{
      df<-country_df[country_df$constituent==input$constituent_twitter,c('count','avg_sentiment','country')]
      }
    
    ##Find the minimum/maximum of the sentiment, in order to decide the range of colors.
    min_sent<-min(df$avg_sentiment, na.rm=T)
    max_sent<-max(df$avg_sentiment, na.rm=T)
    if ((min_sent<=0)&(max_sent>=0)){palette_color = c('red','#EB984E','#FFCC00','#1E8449')}
    if ((min_sent<=0)&(max_sent<=0)){palette_color = c('red','#EB984E','#FFCC00')}
    if ((min_sent>=0)&(max_sent>=0)){palette_color = c('#FFCC00','#1E8449')}
    
    n <- joinCountryData2Map(df, joinCode="ISO2", nameJoinColumn="country")
    mapCountryData(n, nameColumnToPlot="avg_sentiment", mapTitle=title_str,colourPalette=rwmGetColours(palette_color,length(palette_color)))
  })
  
  ##Frequency Mapping
  output$popularity_map<-renderPlot({
    title_str = paste('Tweeting frequency for ',input$constituent_twitter,' by countries',sep='')
    if (input$constituent_twitter=='Adidas'){
      df<-country_df[country_df$constituent==tolower(input$constituent_twitter),c('count','avg_sentiment','country')]}else{
        df<-country_df[country_df$constituent==input$constituent_twitter,c('count','avg_sentiment','country')]
      }
    
    n <- joinCountryData2Map(df, joinCode="ISO2", nameJoinColumn="country")
    mapCountryData(n, nameColumnToPlot="count", mapTitle=title_str,colourPalette=rwmGetColours(c('#F5EEF8','#D7BDE2','#9B59B6','#7D3C98'),4))
  })
  
  
  ########################################## News #########################################
  ##Top Words - World Cloud
  top_words_data <- mongo(collection = 'top_words',
                          url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                          verbose = FALSE, options = ssl_options())
  top_words_df <- top_words_data$find()
  output$word_cloud <-renderPlot({
    if (input$constituent_news=='Adidas'){
      df<-top_words_df[top_words_df$Constituent==tolower(input$constituent_news),c('Words','Frequency')]}else{
      df<-top_words_df[top_words_df$Constituent==(input$constituent_news),c('Words','Frequency')]}
    
    d <- tail(df[,c('Words','Frequency')])
    set.seed(1234)
    wordcloud(words = d$Words, freq = d$Frequency, min.freq = 1,
              max.words=200, random.order=FALSE, rot.per=0.35,
              colors=brewer.pal(8, "Dark2"))
  })
  
  ##News Tagging Count - Multicolored Vertical Bar Chart
  data <- mongo(collection = 'news_tag',
                url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                verbose = FALSE, options = ssl_options())
  news_df = data$find()
  
  output$news_tag_bar<-renderPlot({
    title_str = paste('News Tagging Count of ',input$constituent_news, sep='')
    if (input$constituent_news=='Deutsche Bank'){
      df<-news_df[news_df$constituent==(input$constituent_news),c('tag','count')]}else{
      df<-news_df[news_df$constituent==tolower(input$constituent_news),c('tag','count')] }
    
    ggplot(df,aes(x=df$tag,y=df$count,fill=df$tag))+geom_col()+labs(y="Count of news",x="News tagging") +
    geom_text(aes(label=df$count),hjust=0.5,vjust=0)+ggtitle(title_str) +scale_fill_discrete(guide=FALSE)+
      theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
    
    })
  
  
  ##News Sentiment Trend - Line Graph
  output$news_sentiment_daily<-renderPlot({
    if(input$constituent_news=='Adidas'){
      daily_news <- mongo(collection = 'news_daily_sent_adidas',
                          url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                          verbose = FALSE, options = ssl_options())
    }
    
    if(input$constituent_news=='BMW'){
      daily_news <- mongo(collection = 'news_daily_sent_bmw',
                          url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                          verbose = FALSE, options = ssl_options())
    }
    
    if(input$constituent_news=='Commerzbank'){
      daily_news <- mongo(collection = 'news_daily_sent_commerzbank',
                          url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                          verbose = FALSE, options = ssl_options())
    }
    if(input$constituent_news=='Deutsche Bank'){  
      daily_news <- mongo(collection = 'news_daily_sent_deutsche_bank',
                          url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                          verbose = FALSE, options = ssl_options())}
    
    if(input$constituent_news=='EON'){
      daily_news <- mongo(collection = 'news_daily_sent_eon',
                          url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                          verbose = FALSE, options = ssl_options())
    }
  
    df<-daily_news$find()
    df$date <- as.Date(df$date,"%Y-%m-%d")
    
    ggplot(data=df, aes(x=df$date, y=df$scorescore,group=1))+geom_line()+
      geom_point()+
      labs(y="Average Twitter Sentiment",x="Date")+
      labs(title = title_str)+
      ylim(-1,1)+
      labs(title = paste("Daily News Sentiment for ",input$constituent_news,sep=""))+
      theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
    
  })
  
  
  ##News Sentiment Trend by Categories - Line Graph
  news_tag <- mongo(collection = 'news_tagging_score',
                    url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())
  news_tag_df<-news_tag$find('{}')
  
  ##Average daily sentiment by category
  output$news_sentiment_tag<-renderPlot({
    if (input$constituent_news=='Deutsche Bank'){df<-news_tag_df[news_tag_df$constituent==input$constituent_news,]}else{
    df<-news_tag_df[news_tag_df$constituent==tolower(input$constituent_news),]}
    #df$date<-gsub("-", "/", df$date)
    df$date <- as.Date(df$date,"%Y-%m-%d")
    ##Only focus on shares, investment and stock related news
    title_str = paste('Daily Average News Sentiment for ', input$constituent_news, ' by Categories',sep='')
    #df_selected = df[(df$categorised_tag=='Stock'|df$categorised_tag=='Investment'|df$categorised_tag=='Shares'),]
    ggplot(data = df,aes(x=df$date,y=df$scorescore,group=df$categorised_tag)) +
      scale_y_continuous(limits=c(-1,1))+
      geom_point(aes(color=categorised_tag))+geom_line(aes(color=categorised_tag))+
      ggtitle(title_str)+labs(y="Daily average sentiment score",x="Date")+
      scale_fill_discrete(name = "News Category")+
      theme(legend.position="bottom",legend.direction='horizontal')+
      labs(colour = "Category") +
      theme(plot.title = element_text(hjust = 0.5))+theme(plot.title = element_text(lineheight=.8, face="bold"))
  })
  
  ################################# Correlation Page ######################################
  ### News Sentiment Line 
  output$news_behavior_line <- renderPlotly({
    if(input$constituent_corr=='Adidas'){
      corr <- mongo(collection = 'ADS_cor',
                    url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())}
    
    if(input$constituent_corr=='BMW'){
      corr <- mongo(collection = 'BMW_cor',
                    url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())}
    if(input$constituent_corr=='Deutsche Bank'){
      corr <- mongo(collection = 'DB_cor',
                    url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())}
    if(input$constituent_corr=='Commerzbank'){
      corr <- mongo(collection = 'CB_cor',
                    url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())}
    if(input$constituent_corr=='EON'){
      corr <- mongo(collection = 'EON_cor',
                    url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())}
    
    corr = corr$find()
    if(input$constituent_corr=='Adidas'){
      mydata <- corr[, c('News_sent','Close','Open','High','Low','date')]
    }else{mydata <- corr[, c('News_sent','Close','Open','High','Low','date')]}
    
    title_str = paste('Behavior of ',input$constituent_corr, ' prices relative to News Sentiments',sep='' )
    
    Close = mydata$Close
    High = mydata$High
    Low = mydata$Low
    Open = mydata$Open
    News_sent = mydata$News_sent
    
    O <- cor(News_sent, Open)
    C <- cor(News_sent, Close)
    L <- cor(News_sent, Low)
    H <- cor(News_sent, High)
    
    p <- ggplot(mydata, aes(x = date, group=1))
    
    p <- p + {if (C > 0.5) geom_line(aes(y = Close), linetype = "dashed", colour = "black")}
    p <- p + {if (O > 0.5) geom_line(aes(y = Open), linetype = "dashed", colour = "red")}
    p <- p + {if (H > 0.5) geom_line(aes(y = High), linetype = "dashed", colour = "blue")}
    p <- p + {if (L > 0.5) geom_line(aes(y = Low), linetype = "dashed", colour = "orange")}
    
    
    ##add annotations and scaling
    if(input$constituent_corr=='Adidas'){
      p <- p + geom_line(aes(y = News_sent*270, colour = "News Sentiment"))
      p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.85, 0.950)) + labs(title = "Behaviour of Adidas Prices Relative to News Sentiment") 
    }
    
    if(input$constituent_corr=='BMW'){
      p <- p + geom_line(aes(y = News_sent*370, colour = "News Sentiment"))
      p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = "Behaviour of BMW Prices Relative to News Sentiment") 
    }
    
    if(input$constituent_corr=='Commerzbank'){
      p <- p + geom_line(aes(y = News_sent*70, colour = "News Sentiment"))
      p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = "Commerzbank Prices Relative to News Sentiment") 
    }
    
    if(input$constituent_corr=='Deutsche Bank'){
      p <- p + geom_line(aes(y = News_sent*270, colour = "News Sentiment"))
      p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = "Deutschebank Prices Relative to News Sentiment") 
    }
    
    if(input$constituent_corr=='EON'){
      p <- p + geom_line(aes(y = News_sent*56, colour = "News Sentiment"))
      p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = "Behaviour of E.ON Stock Prices Relative to News Sentiment") 
    }
    p <- p + theme(plot.title=element_text(size=10))
    ggplotly(p)
    p_changed <- ggplotly(p)
    pp_changed=plotly_build(p_changed)   
    style( pp_changed ) %>% 
      layout( legend = list(x = 0.01, y = 0.95) )
    
  })
  
  
  
  
  
  ## Twitter Sentiment line
  output$twitter_behavior_line <- renderPlotly({
    if(input$constituent_corr=='Adidas'){
      corr <- mongo(collection = 'ADS_cor',
                    url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())}
    
    if(input$constituent_corr=='BMW'){
      corr <- mongo(collection = 'BMW_cor',
                    url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())}
    if(input$constituent_corr=='Deutsche Bank'){
      corr <- mongo(collection = 'DB_cor',
                    url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())}
    if(input$constituent_corr=='Commerzbank'){
      corr <- mongo(collection = 'CB_cor',
                    url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())}
    if(input$constituent_corr=='EON'){
      corr <- mongo(collection = 'EON_cor',
                    url = "mongodb://igenie_readwrite:igenie@35.197.204.103:27017/dax_gcp",
                    verbose = FALSE, options = ssl_options())}
    
    corr = corr$find()
    if(input$constituent_corr=='Adidas'){
      mydata <- corr[, c('Twitter_sent','Close','Open','High','Low','date')]
    }else{mydata <- corr[, c('Twitter_sent','Close','Open','High','Low','date')]}
    
    
    Close = mydata$Close
    High = mydata$High
    Low = mydata$Low
    Open = mydata$Open
    Twitter_sent = mydata$Twitter_sent
    
    O <- cor(Twitter_sent, Open)
    C <- cor(Twitter_sent, Close)
    L <- cor(Twitter_sent, Low)
    H <- cor(Twitter_sent, High)
    
    p <- ggplot(mydata, aes(x = date, group=1))
    
    p <- p + {if (C > 0.5) geom_line(aes(y = Close), linetype = "dashed", colour = "black")}
    p <- p + {if (O > 0.5) geom_line(aes(y = Open), linetype = "dashed", colour = "red")}
    p <- p + {if (H > 0.5) geom_line(aes(y = High), linetype = "dashed", colour = "blue")}
    p <- p + {if (L > 0.5) geom_line(aes(y = Low), linetype = "dashed", colour = "orange")}
    
    
    ##add annotations and scaling
    if(input$constituent_corr=='Adidas'){
      p <- p + geom_line(aes(y = Twitter_sent*270, colour = "Twitter Sentiment"))
      p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = "Behaviour of Adidas Prices Relative to Sentiment") 
    }
    
    if(input$constituent_corr=='BMW'){
      p <- p + geom_line(aes(y = Twitter_sent*150, colour = "Twitter Sentiment"))
      p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = "Behaviour of BMW Stock Prices Relative to Twitter Sentiment") 
    }
    
    if(input$constituent_corr=='Commerzbank'){
      p <- p + geom_line(aes(y = Twitter_sent*40, colour = "Twitter Sentiment"))
      p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = "Behaviour of Commerzbank Prices Relative to Sentiment") 
    }
    
    if(input$constituent_corr=='Deutsche Bank'){
      p <- p + geom_line(aes(y = Twitter_sent*50, colour = "Twitter Sentiment"))
      p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = "Behaviour of Deutschebank Prices Relative to Sentiment") 
    }
    
    if(input$constituent_corr=='EON'){
      p <- p + geom_line(aes(y = Twitter_sent*12.5, colour = "Twitter Sentiment"))
      p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = "Behaviour of E.ON Prices Relative to Twitter Sentiment") 
    }
    p <- p + theme(plot.title=element_text(size=10))
    ggplotly(p)
    p_changed <- ggplotly(p)
    pp_changed=plotly_build(p_changed)   
    style( pp_changed ) %>% 
      layout( legend = list(x = 0.01, y = 0.95) )
  })
  
  ### Add explanations
  output$news_annotation<- renderText({
    if(input$constituent_corr=='Adidas'){
      str = "Correlations for all Adidas stock prices have less than 50% correlation to News sentiment"
    }
    if(input$constituent_corr=='BMW'){
      str = "Open, Close, High and Low prices all have greater than 50% correlations to News sentiment. News sentiment had the greatest correlations to Highs, which had a 99% correlation, followed by Open and lows, which both were 97% correlated to sentiment, and finally, Close prices, which were 90% correlated."
    }
    if(input$constituent_corr=='Commerzbank'){
      str = "Open, Close, High and Low prices all have greater than 50% correlations to News sentiment. News sentiment had the greatest correlations to Highs and Close Prices, which were both 99% correlated, followed by Lows, which was 91% correlated to sentiment, and finally, Open prices, which was 84% correlated."
    }
    if(input$constituent_corr=='Deutsche Bank'){
      str = "Open, Close, High and Low prices all have greater than 50% correlations to News sentiment. News sentiment had the greatest correlations to Open and Close Prices, which were 93% correlated, followed by Lows, which was 90% correlated to sentiment, and finally, Highs, which was 71% correlated."
    }
    if(input$constituent_corr=='EON'){
      str = "Only Close, Open and Lows have greater than 50% correlations to News sentiment. News sentiment had the greatest correlation to Lows, which had a 100% correlation, followed by Close, which was 93% correlated to sentiment, and finally, Open Prices, which were 54% correlated."
    }
    str
  })
  output$twitter_annotation<- renderText({
    if(input$constituent_corr=='Adidas'){
      str = "Only Adidas open prices seem to have greater than 50% correlation to Twitter sentiment. Open prices have a 78% correlation to Twitter sentiment for the total time period shown.
"
    }
    if(input$constituent_corr=='BMW'){
      str = "Open, Close, High and Low prices all have greater than 50% correlations to Twitter sentiment. Twitter sentiment had the greatest correlations to Close prices, which had a 99% correlation, followed by Open and lows, which both were 95% correlated to sentiment, and finally, Highs, which were 92% correlated."
    }
    if(input$constituent_corr=='Commerzbank'){
      str = "Open, Close, High and Low prices all have greater than 50% correlations to Twitter sentiment. Twitter sentiment had the greatest correlation to Open Prices, which was 98% correlated, followed by Highs, which was 90% correlated to sentiment. Close prices were 89 percent correlated and Lows were 69% correlated."
    }
    if(input$constituent_corr=='Deutsche Bank'){
      str = "Only Close, High and Low prices have greater than 50% correlations to Twitter sentiment. Twitter sentiment had the greatest correlations to Highs, which had a 98% correlation, followed by Low, which was 86% correlated to sentiment, and finally, Close Prices, which were 83% correlated."
    }
    if(input$constituent_corr=='EON'){
      str = "Only Open and Highs have greater than 50% correlations to Twitter sentiment. Twitter sentiment had the greatest correlation to Open prices, which had a 99% correlation, while Highs were 94% correlated."
    }
    str
  })
  
  
}

 

