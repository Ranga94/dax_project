library(wordcloud)
library(RColorBrewer)
library(plotly)
library(treemap)
library(jsonlite)

options(scipen=1000)
library(DT)
library(shiny)
library(shinydashboard)

##Create a function that color codes table
##COLOR-CODED TABLE
library(bigrquery)
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

server <- function(input, output){
  
  #set_service_token("/Users/SparkingAries/Documents/GitHub/testfolder/igenie-project-key.json")
  #set_service_token('/Users/kefei/Documents/Igenie_Consulting/keys/igenie-project-key.json')
  set_service_token("igenie-project-key.json")
  project <- "igenie-project" 
  #from_date <- as.Date('2017-11-17')
  #to_date <- as.Date('2017-12-01')
  from_date <- '2017-11-17 00:00:00'
  to_date <-'2017-12-01 00:00:00'
  
  ######################################  Main content of the dashboard  ##############################################
  
  ##############################    HOMEPAGE    ###################################
  
  ##Popular tweets
  
  #Treemap
  output$popular_treemap<-renderPlot({
    sql <- 'SELECT * FROM[igenie-project:pecten_dataset_test.twitter_sentiment_popularity];'
    #sql<- paste("SELECT * FROM pecten_dataset_test.twitter_sentiment_popularity AND date between TIMESTAMP('", from_date, " UTC')",
            #" and TIMESTAMP('" ,to_date, " UTC') ;",sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    retrieved_data<-na.omit(retrieved_data)
    retrieved_data$to_date<-strptime(retrieved_data$to_date,format = "%Y-%m-%d")
    retrieved_data$from_date<-strptime(retrieved_data$from_date,format = "%Y-%m-%d")
   
    retrieved_data<-retrieved_data[retrieved_data$from_date==as.Date(from_date) & retrieved_data$to_date == as.Date(to_date),]
    popular_tweet_treemap(retrieved_data)
  })
  
 # News
   #from_date_temp <- as.integer(as.POSIXct(strptime(from_date,"%Y-%m-%d"))) * 1000
   #to_date_temp <- as.integer(as.POSIXct(strptime(to_date,"%Y-%m-%d"))) * 1000

  sql <- 'SELECT From_date, To_date, constituent,NEWS_TITLE_NewsDim,NEWS_DATE_NewsDim,NEWS_ARTICLE_TXT_NewsDim,categorised_tag,sentiment FROM[igenie-project:pecten_dataset_test.news_all];'
  news_all_data <- query_exec(project=project,  sql, billing = project)
  news_all_data$From_date<- strptime(news_all_data$From_date,format = "%Y-%m-%d")
  news_all_data$To_date<-strptime(news_all_data$To_date,format = "%Y-%m-%d")
  news_all_data <- news_all_data[news_all_data$From_date==as.Date(from_date) & news_all_data$To_date==as.Date(to_date),]
  news_all_data <- news_all_data[!is.na(news_all_data$From_date),]

  
  news_data_all <- eventReactive(input$reload, {
     ##Sort by alphabetic order of constituents
    news_all_data <- news_all_data[order(news_all_data$constituent),]
     news_transform(news_all_data)
   }, ignoreNULL = FALSE)
   output$news_all <- DT::renderDataTable(news_data_all(),selection = 'single', server=FALSE)
   
   observeEvent(input$news_all_rows_selected,
                {
                  i = input$news_all_rows_selected
                  cat(i)
                  i <- i[length(i)]
                  retrieved_data <- news_all_data[order(news_all_data$constituent),]
                  showModal(modalDialog(
                    
                    title = toString(retrieved_data[i,c('NEWS_TITLE_NewsDim')]),
                    #title = toString(retrieved_data[i,c('constituent')]),
                    toString(retrieved_data[i,c('NEWS_ARTICLE_TXT_NewsDim')])
                ))
               })

   
  
  ##For analyst opinions
  sql <- 'SELECT * FROM[igenie-project:pecten_dataset_test.analyst_opinions];'
  analyst_opinions <- query_exec(project=project,  sql, billing = project)
  #analyst_opinions$Date<-strptime(analyst_opinions$Date,format = "%Y-%m-%d %H:%M:%S")
  analyst_opinions$Date<-strptime(analyst_opinions$Date,format = "%Y-%m-%d")
  analyst_opinions<-analyst_opinions[analyst_opinions$Date>=as.Date(from_date) & analyst_opinions$Date<= as.Date(to_date),]
  
  ## Analyst Recommendation Percentage - Stacked Bar Chart
  output$analystplot1 <- renderPlot({
    analyst_stacked_bar_1(analyst_opinions)
  })
  
  output$analystplot2 <- renderPlot({
    analyst_stacked_bar_2(analyst_opinions)
  })
  
  output$analystplot3 <- renderPlot({
    analyst_stacked_bar_3(analyst_opinions)
  })
  
  
  ##Summary Box - DataTable
  
  sql <- 'SELECT * FROM[igenie-project:pecten_dataset_test.summary_box];'
  sql <- paste("SELECT * FROM[igenie-project:pecten_dataset_test.summary_box] WHERE From_date = '", from_date ,"';",sep='')
  retrieved_summary_data <- query_exec(project=project,  sql, billing = project)
  retrieved_summary_data <- retrieved_summary_data[rowSums(is.na(retrieved_summary_data)) == 0,]
  retrieved_summary_data<-retrieved_summary_data[order(retrieved_summary_data$Constituent),]
  
  
  output$summarytable1 <- renderDataTable({
    summary_box_1(retrieved_summary_data)               
  })
  
  output$summarytable2 <- renderDataTable({
    summary_box_2(retrieved_summary_data)               
  })
  
  output$summarytable3 <- renderDataTable({
    summary_box_3(retrieved_summary_data)               
  })
  
  
  
  ####################################### FUNDAMENTAL ###########################################
  
  ##Cumulative Returns - DataTable
  output$CRtable  <- renderDataTable({
    sql <- paste("SELECT * FROM [igenie-project:pecten_dataset_test.cumulative_returns] WHERE To_date= '", to_date ,"';", sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    retrieved_data<-na.omit(retrieved_data)
    retrieved_data$From_date<- strptime(retrieved_data$From_date,format = "%Y-%m-%d %H:%M:%S")
    retrieved_data$To_date<-strptime(retrieved_data$To_date,format = "%Y-%m-%d %H:%M:%S")
    retrieved_data[retrieved_data$From_date==as.Date(from_date) & retrieved_data$To_date==as.Date(to_date),]
    cumulative_return_table(retrieved_data)
  })
  
  
  ##Profitability Ranking - DataTable
  output$ranking_top  <- renderDataTable({
    sql <- paste("SELECT * FROM[igenie-project:pecten_dataset_test.Profitability_tag_ranking] WHERE To_date= '", to_date ,"';", sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    retrieved_data<-na.omit(retrieved_data)
    rank_n_tag(retrieved_data)
  })
  
  
  
  ##EPS Analysis - DataTable
  output$EPS_table <- renderDataTable({
    sql <- 'SELECT Constituent,Current_EPS,EPS_last_year FROM[igenie-project:pecten_dataset_test.EPS_t] WHERE STATUS ="active";'
    EPS_data <- query_exec(project=project,  sql, billing = project)
    EPS_table(EPS_data)
  })
  
  ##PER Analysis - DataTable
  output$PER_table <- renderDataTable({
    sql <- 'SELECT Constituent,Current_PER,PER_last_year FROM[igenie-project:pecten_dataset_test.PER_t] WHERE STATUS ="active";'
    PER_data <- query_exec(project=project,  sql, billing = project)
    PER_table(PER_data)
  })
  
  
  ##################################### ANALYST PAGE ####################################
  ##Analyst Recommendation - DataTable
  
  ##Use the analyst data queried in the Home Page Section
  output$recommendation_table <- renderDataTable({
    analyst_rating_table(analyst_opinions)
  }) 
  
  ##Analyst Target Prices - DataTable
  output$target_price_table <- renderDataTable({
    analyst_target_prices(analyst_opinions)
  })
  
  
  
  ####################################### Risk Analysis ###########################################
  output$var_chart <- renderPlot({
  #   #step1 get the input ready
    #input$submit
     maxDate <- input$startdate
     tickers <- input$portfolio
     weights <- as.numeric(input$weights)
     n <- length(tickers)
     meth <- input$meth
     var_table <- value_at_risk(maxDate,tickers, weights, n, meth)
     
     ggplot(var_table, aes(x=Assets, y=VaR, fill=Assets)) + geom_bar(stat = "identity", position = "dodge") #+ scale_fill_manual(values = "Grey50", limits = 4)
   })
  # 
  
  ##############################  TWITTER PAGE #######################################
  output$target_price_error <- renderText({
    if (constituent =="Adidas"){constituent = 'adidas'}
    sql <- paste('SELECT * FROM[igenie-project:pecten_dataset_test.target_prices] WHERE constituent ="',constituent,'";',sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    p<-nrow(retrieved_data)
    p
    })
  
  output$general_twitter_target_price<-renderPlot({
    
    constituent = toString(input$constituent)
    if (constituent =="Adidas"){constituent = 'adidas'}
    ## No new twitter target prices
    sql <- paste('SELECT * FROM[igenie-project:pecten_dataset_test.target_prices] WHERE constituent ="',constituent,'";',sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    retrieved_data$from_date<-strptime(retrieved_data$from_date,format = "%Y-%m-%d" )
    retrieved_data$to_date<-strptime(retrieved_data$to_date,format = "%Y-%m-%d")
    retrieved_data<-retrieved_data[retrieved_data$from_date==as.Date(from_date)&retrieved_data$to_date==as.Date(to_date),]
    #retrieved_data<-na.omit(retrieved_data)
    nrows <- nrow(retrieved_data)
    if (nrows == 0){
      retrieved_data <- query_exec(project=project,  sql, billing = project)
      retrieved_data<-na.omit(retrieved_data)
    }
    general_target_price_bar(retrieved_data,constituent)
  })
  
  
  output$influencer_price_error <- renderText({
    if (constituent =="Adidas"){constituent = 'adidas'}
    sql <- paste('SELECT * FROM[igenie-project:pecten_dataset_test.influencer_prices] WHERE constituent ="',constituent,'";',sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    p<-nrow(retrieved_data)
    p
  })
  
  
  
  #Deutsche Borse, bmw, henkel, infenion,Volkswagen shows error
  output$influencer_twitter_target_price<-renderPlot({
    # No new twitter target prices
    constituent = toString(input$constituent)
    if (constituent =="Adidas"){constituent = 'adidas'}
    sql <- paste('SELECT * FROM[igenie-project:pecten_dataset_test.influencer_prices] WHERE constituent ="',constituent,'";',sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    retrieved_data$from_date<-strptime(retrieved_data$from_date,format = "%Y-%m-%d" )
    retrieved_data$to_date<-strptime(retrieved_data$to_date,format = "%Y-%m-%d")
    retrieved_data<-retrieved_data[retrieved_data$from_date==as.Date(from_date)&retrieved_data$to_date==as.Date(to_date),]
    nrows <- nrow(retrieved_data)
    if (nrows == 0){
      retrieved_data <- query_exec(project=project,  sql, billing = project)
      retrieved_data<-na.omit(retrieved_data)
    }
    influencer_target_price_bar(retrieved_data,constituent)
  })
  
  ##Count the number of tweets for different sentiments
  output$tweet_num <-renderPlot({
    constituent = toString(input$constituent)
    if (constituent =="Adidas"){constituent = 'adidas'}
    #sql <- 'SELECT * FROM[igenie-project:pecten_dataset_test.twitter_sentiment_count_daily];'
    sql <- paste('SELECT * FROM[igenie-project:pecten_dataset_test.twitter_sentiment_count_daily] WHERE constituent ="',constituent,'";', sep='')
    twitter_counts <- query_exec(project=project,  sql, billing = project)
    twitter_counts<-na.omit(twitter_counts)
    twitter_counts$From_date<- strptime(twitter_counts$From_date,format = "%Y-%m-%d %H:%M:%S")
    twitter_counts$To_date<- strptime(twitter_counts$To_date,format = "%Y-%m-%d %H:%M:%S")
    twitter_counts<-twitter_counts[twitter_counts$From_date == as.Date(from_date) & twitter_counts$To_date == as.Date(to_date),]
    #twitter_counts$date<-strptime(twitter_counts$date,format = "%Y-%m-%d %H:%M:%S")
    #twitter_counts<- twitter_counts[twitter_counts$From_date==as.Date(from_date) & twitter_counts$To_date==as.Date(to_date),]
    twitter_counts<- unique(twitter_counts)
    tweet_count(twitter_counts, constituent)
  })
  
  
  ##World Twitter Data  - Map Plot
  ##Prosiebensat1, Vonovia, Volkswagen shows error
  output$sentiment_map<-renderPlot({
    constituent = toString(input$constituent)
    if (constituent =="Adidas"){constituent = 'adidas'}
    sql <- paste('SELECT count,avg_sentiment,country_name,constituent,from_date,to_date FROM[igenie-project:pecten_dataset_test.country_data] WHERE constituent ="',constituent,'";', sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    
    #retrieved_data$date_of_analysis<-strptime(retrieved_data$date_of_analysis,format = "%Y-%m-%d")
    retrieved_data$from_date<-strptime(retrieved_data$from_date,format = "%Y-%m-%d" )
    retrieved_data$to_date<-strptime(retrieved_data$to_date,format = "%Y-%m-%d")
    #Select the data according to the date range
    df<-retrieved_data[retrieved_data$from_date==as.Date(from_date)&retrieved_data$to_date==as.Date(to_date),]
    df<-na.omit(df)
    nrows <- nrow(df)
    if (nrows <2){
     df<-retrieved_data
    }
    
    map_sentiment(df,constituent)
  })
  
  ##Frequency Mapping
  ##dummy
  
 
  
  output$popularity_map<-renderPlot({
     constituent = toString(input$constituent)
     sql <-paste('SELECT count,avg_sentiment,country_name,constituent,from_date,to_date FROM[igenie-project:pecten_dataset_test.country_data] WHERE constituent="',constituent,'";', sep='')
     retrieved_data <- query_exec(project=project,  sql, billing = project)
     retrieved_data$from_date<-strptime(retrieved_data$from_date,format = "%Y-%m-%d" )
     retrieved_data$to_date<-strptime(retrieved_data$to_date,format = "%Y-%m-%d")
     #select the data according to the date range
     df<-retrieved_data[retrieved_data$from_date==as.Date(from_date)&retrieved_data$to_date==as.Date(to_date),]
     df<-na.omit(df)
     nrows <- nrow(df)
     if (nrows <2 ){
       df<-retrieved_data
     }
     map_frequency(retrieved_data, constituent)
  })
  
  ##Most Recent Tweets
  output$recent_tweets_table <- renderDataTable({
    constituent = toString(input$constituent)
    if (constituent =="Adidas"){constituent = 'adidas'}
    sql <- paste('SELECT * FROM[igenie-project:pecten_dataset_test.twitter_analytics_latest_price_tweets] WHERE constituent="',constituent,'";', sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    #删去重复text
    retrieved_data <- retrieved_data[!duplicated(retrieved_data$text),]
    #筛选日期
    retrieved_data$from_date<-strptime(retrieved_data$from_date,format = "%Y-%m-%d" )
    retrieved_data$to_date<-strptime(retrieved_data$to_date,format = "%Y-%m-%d")
    retrieved_data<-retrieved_data[retrieved_data$from_date==as.Date(from_date)&retrieved_data$to_date==as.Date(to_date),]
    #retrieved_data<-na.omit(retrieved_data)
    nrows <- nrow(retrieved_data)
    if (nrows == 0){
      retrieved_data <- query_exec(project=project,  sql, billing = project)
      retrieved_data <- retrieved_data[!duplicated(retrieved_data$text),]
    }
    recent_tweets(retrieved_data)
  })
  
  
  ########################################## News #########################################
  
  ##News Tagging Count - Multicolored Vertical Bar Chart
  output$news_tag_bar<-renderPlot({
    constituent <-toString(input$constituent)
    if (constituent == 'Adidas'){constituent = "adidas"}
    sql <- paste('SELECT From_date,To_date,Tags,Count FROM[igenie-project:pecten_dataset_test.news_tag] WHERE constituent="',constituent,'";', sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    #retrieved_data$Date <-as.Date(retrieved_data$Date)
    retrieved_data<-retrieved_data[retrieved_data$Tags!='None',]
    retrieved_data$To_date<-strptime(retrieved_data$To_date,format = "%Y-%m-%d")
    retrieved_data$From_date<-strptime(retrieved_data$From_date,format = "%Y-%m-%d")
    retrieved_data <-retrieved_data[retrieved_data$From_date == as.Date(from_date) & retrieved_data$To_date==as.Date(to_date) ,]
    retrieved_data<-na.omit(retrieved_data)
    if (constituent == 'adidas'){constituent = "Adidas"}
    count_tags_bar(retrieved_data ,constituent)
  })
  
  
  ##News Sentiment Trend - Line Graph
  output$news_sentiment_daily<-renderPlot({
    constituent <-toString(input$constituent)
    if (constituent == 'Adidas'){constituent = "adidas"}
    sql <- paste('SELECT * FROM[igenie-project:pecten_dataset_test.news_analytics_daily_sentiment] WHERE constituent="',constituent,'";', sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    retrieved_data$To_date<-strptime(retrieved_data$To_date,format = "%Y-%m-%d")
    retrieved_data$From_date<-strptime(retrieved_data$From_date,format = "%Y-%m-%d")
    #retrieved_data$date <-as.Date(retrieved_data$date)
    retrieved_data <-retrieved_data[retrieved_data$From_date == as.Date(from_date) & retrieved_data$To_date==as.Date(to_date),]
    daily_news_sent(retrieved_data,constituent)
  })
  
  
  ##News sentiment by Category - Heatmap
  output$topic_sentiment_grid<-renderPlot({
    constituent <-toString(input$constituent)
    sql <- paste('SELECT * FROM[igenie-project:pecten_dataset_test.news_analytics_topic_sentiment] WHERE constituent="',constituent,'";', sep='')
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    retrieved_data$date<- strptime(retrieved_data$date,format = "%Y-%m-%d %H:%M:%S")
    constituent <-toString(input$constituent)
    topic_sentiment_grid_plot(retrieved_data,constituent)
  })
  
  
  #sql <- 'SELECT NEWS_DATE_NewsDim,constituent,categorised_tag,NEWS_TITLE_NewsDim, NEWS_ARTICLE_TXT_NewsDim FROM[igenie-project:pecten_dataset_test.news_analytics_topic_articles];'
  sql<- paste("SELECT From_Date,To_Date, NEWS_DATE_NewsDim,constituent,categorised_tag,News_Title_NewsDim, NEWS_ARTICLE_TXT_NewsDim FROM[igenie-project:pecten_dataset_test.news_analytics_topic_articles] WHERE From_Date =TIMESTAMP('", from_date, " UTC') AND To_Date = TIMESTAMP('", to_date, " UTC') ;",sep='')
  news_article_data <- query_exec(project=project,  sql, billing = project)
  news_article_data <-news_article_data[!is.na(news_article_data$From_Date),]
  
   news_analytics_topic_articles_all <- eventReactive(input$constituent, {
     constituent = toString(input$constituent)
     if (constituent == 'Adidas'){constituent = "adidas"}
     if (constituent == 'Thyssenkrupp'){constituent = "thyssenkrupp"}
     df<- news_article_data[news_article_data$constituent == constituent,]
     news_analytics_topic_articles_func(df,constituent)
   }, ignoreNULL = FALSE)
   
   output$news_analytics_topic_articles <- DT::renderDataTable(news_analytics_topic_articles_all(),
                                                               selection = 'single', server=FALSE)
   
   observeEvent({input$news_analytics_topic_articles_rows_selected},
                {
                  i = input$news_analytics_topic_articles_rows_selected
                  cat(i)
                  i <- i[length(i)]
                  
                  constituent = toString(input$constituent)
                  if (constituent == 'Adidas'){constituent = "adidas"}
                  if (constituent == 'Thyssenkrupp'){constituent = "thyssenkrupp"}
                  db<-news_article_data[news_article_data$constituent== constituent,]
                  showModal(modalDialog(
                    #title = constituent 
                    title = toString(db[i,c('News_Title_NewsDim')]),
                    toString(db[i,c('NEWS_ARTICLE_TXT_NewsDim')])
                    #df[i,]
                  ))
                })
   
 
  
  
  ################################# Correlation Page ######################################
  sql <- 'SELECT * FROM[igenie-project:pecten_dataset_test.all_correlations];'
  correlation_data <- query_exec(project=project,  sql, billing = project)
  #correlation_data$Date<-as.Date(correlation_data$Date)
  #correlation_data$Date<-strptime(correlation_data$Date,format = "%Y-%m-%d")
  #correlation_data<-na.omit(correlation_data)
  correlation_data<-na.omit(correlation_data)
  #correlation_data$From_date<- strptime(correlation_data$From_date,format = "%Y-%m-%d %H:%M:%S")
  #correlation_data$To_date<- strptime(correlation_data$To_date,format = "%Y-%m-%d %H:%M:%S")
  correlation_data<-correlation_data[correlation_data$From_date==as.Date(from_date) & correlation_data$To_date==as.Date(to_date),]
  
  ## News Sentiment line
  output$news_behavior_line <- renderPlotly({
    constituent = toString(input$constituent)
    df<-correlation_data[correlation_data$Constituent==constituent,]
    correlation_news(df,constituent)
  })
  
  
  ## Twitter Sentiment line
  output$twitter_behavior_line <- renderPlotly({
    constituent = toString(input$constituent)
    df<-correlation_data[correlation_data$Constituent==constituent,]
    correlation_twitter(df,constituent)
  })
  
  
  ### Add explanations
  output$news_annotation<- renderText({
    constituent = toString(input$constituent)
    news_annotation_selection(constituent)
  })
  
  output$twitter_annotation<- renderText({
    constituent = toString(input$constituent)
    twitter_annotation_selection(constituent)
  })
  
  }