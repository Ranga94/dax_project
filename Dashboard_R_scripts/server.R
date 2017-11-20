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
source('Homepage_subfunctions.R')
source('Fundamental_subfunctions.R')
source('Analyst_subfunctions.R')
source('Twitter_subfunctions.R')
source('News_subfunctions.R')
source('Correlation_subfunctions.R')

server <- function(input, output){
  url_mongo <- "mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp"
  
######################################  Main content of the dashboard  ##############################################
  
  ##############################    HOMEPAGE    ###################################
  ## The Twitter Sentiment Count - Line plot
  twitter_count <- mongo(collection = 'twitter_sentiment_count_daily',
                         url = url_mongo,
                         verbose = FALSE, options = ssl_options())
  twitter_df<-twitter_count$find('{}')
  twitter_df$date<-as.Date(twitter_df$date)
  twitter_df<- twitter_df[twitter_df$date>as.Date('2017-09-06'),]
  twitter_df<- twitter_df[twitter_df$date<as.Date('2017-09-22'),]
  twitter_df<- unique(twitter_df)
  
  ##BMW
  tweet_count_bmw <-eventReactive(input$reload, {
    constituent_count <- twitter_df[twitter_df$constituent=='BMW',]
    tweet_count(constituent_count)},ignoreNULL = FALSE)
  output$tweet_num_bmw<-renderPlot(tweet_count_bmw())
  
  
  ## Adidas
  tweet_count_adidas <-eventReactive(input$reload, {
    constituent_count <- twitter_df[twitter_df$constituent=='adidas',]
    tweet_count(constituent_count)
  },ignoreNULL = FALSE)
  output$tweet_num_adidas<-renderPlot(tweet_count_adidas())
  
  
  ## Commerzbank
  tweet_count_cb <-eventReactive(input$reload, {
    constituent_count <- twitter_df[twitter_df$constituent=='Commerzbank',]
    tweet_count(constituent_count)
    },ignoreNULL = FALSE)
  output$tweet_num_cb<-renderPlot(tweet_count_cb())
  
  
  ## Deutsche Bank
  tweet_count_db <-eventReactive(input$reload, {
    constituent_count <- twitter_df[twitter_df$constituent=='Deutsche Bank',]
    tweet_count(constituent_count)
  },ignoreNULL = FALSE)
  output$tweet_num_db<-renderPlot(tweet_count_db())
  
  
  ## EON
  tweet_count_eon <-eventReactive(input$reload, {
    constituent_count <- twitter_df[twitter_df$constituent=='EON',]
    tweet_count(constituent_count) 
  },ignoreNULL = FALSE)
  output$tweet_num_eon<-renderPlot(tweet_count_eon())
  
  
  
  ## News Table - DataTable
  from_date <- as.integer(as.POSIXct(strptime("2017-11-10","%Y-%m-%d"))) * 1000
  to_date <- as.integer(as.POSIXct(strptime("2017-11-16","%Y-%m-%d"))) * 1000
  
  query <- paste0('{"NEWS_DATE_NewsDim":{"$gte":{"$date":{"$numberLong":"', from_date,'"}},
                  "$lte":{"$date":{"$numberLong":"', to_date,'"}}},
                  "constituent_id":{"$exists":true},
                  "constituent_id":{"$in":["ADSDE8190216927","CBKDEFEB13190","DBKDEFEB13216","BMWDE8170003036","EOANDE5050056484"]}}')
  
  proyection <- paste0('{"NEWS_ARTICLE_TXT_NewsDim":false}')
  
  news_data_all <- eventReactive(input$reload, {
    all_news <- mongo(collection = 'all_news',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
    db <- all_news$find(query, proyection)
    news_transform(db)
  }, ignoreNULL = FALSE)
  output$news_all <- DT::renderDataTable(news_data_all())
  
    
  
  ## Analyst Recommendation Percentage - Stacked Bar Chart
  analyst_data <- eventReactive(input$reload, {
    db <- mongo(collection = 'analyst_opinions',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find('{"Status":"inactive"}')
    retrieved_data$Date<-as.Date(retrieved_data$Date)
    retrieved_data<-retrieved_data[retrieved_data$Date==as.Date('2017-10-06'),]
    analyst_stacked_bar(retrieved_data)
    }, ignoreNULL = FALSE)
  output$analystplot <- renderPlot(analyst_data())

  
  ##Summary Box - DataTable
  summary_data <- eventReactive(input$reload, {
    db <- mongo(collection = 'summary_box',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find('{}')
    summary_box(retrieved_data)               
  }, ignoreNULL = FALSE)
  output$reactivetable <- renderDataTable(summary_data())
  
  
  
  ####################################### FUNDAMENTAL ###########################################
  ##Cumulative Returns - DataTable
  price_analysis_db <- mongo(collection = 'price analysis',
              url = url_mongo,
              verbose = FALSE, options = ssl_options())
  
  ##Cumulative Returns - DataTable
  cumulative_return <- eventReactive(input$reload, {
    retrieved_data <- price_analysis_db$find('{"Table":"cumulative return analysis","Status":"inactive"}')
    #Select wanted date
    retrieved_data$Date<-as.Date(retrieved_data$Date)
    retrieved_data=retrieved_data[retrieved_data$Date==as.Date('2017-10-08'),]
    cumulative_return_table(retrieved_data)
  },ignoreNULL = FALSE)
  output$CRtable <- renderDataTable(cumulative_return())
  

  ##Golden Cross - DataTable
  golden_cross <- eventReactive(input$reload, {
    retrieved_data <- price_analysis_db$find('{"Table":"Market signal","Status":"inactive"}')
    #select the wanted dates
    retrieved_data$Date<-as.Date(retrieved_data$Date)
    retrieved_data=retrieved_data[retrieved_data$Date==as.Date('2017-10-08'),]
    cross_analysis(retrieved_data)
    },ignoreNULL = FALSE)
  output$cross_table <- renderDataTable(golden_cross())
  
  
  ##Profitability Ranking - DataTable
  ranking <- eventReactive(input$reload, {
    db <- mongo(collection = 'profitability_ranking',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
    retrieved_data<-db$find('{}')
    retrieved_data$Date<-as.Date(retrieved_data$Date)
    retrieved_data=retrieved_data[retrieved_data$Date==as.Date('2017-10-04'),]
    retrieved_data <- db$find('{"Status":"inactive"}')
    rank_n_tag(retrieved_data)
      },ignoreNULL = FALSE)
  output$ranking_top  <- renderDataTable(ranking()) 
 
  
  
  
  ##################################### ANALYST PAGE ####################################
  ##Analyst Recommendation - DataTable
  analyst_recommendation <- eventReactive(input$reload, {
    db <- mongo(collection = 'analyst_opinions_all',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find('{"Status":"active"}')
    analyst_rating_table(retrieved_data)
  },ignoreNULL = FALSE) 
  output$recommendation_table <- renderDataTable(analyst_recommendation())
  
  
  ##Analyst Target Prices - DataTable
  target_prices <- eventReactive(input$reload, {
    db <- mongo(collection = 'analyst_opinions_all',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find('{"Status":"active"}')
    analyst_target_prices(retrieved_data)
     },ignoreNULL = FALSE)
  output$target_price_table <- renderDataTable(target_prices())
  
  
  ##PER Analysis - DataTable
  PER_analysis <- eventReactive(input$reload, {
    db <- mongo(collection = 'fundamental analysis',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
    retrieved_data <- db$find('{"Table":"PER analysis"}')
    PER_table(retrieved_data)
  },ignoreNULL = FALSE)
  output$PER_table <- renderDataTable(PER_analysis())
  
  
  ##############################  TWITTER PAGE #######################################
  ##Twitter Target Price Distribution - Vertical Bar Charts 
  data_twitter <- mongo(collection = 'twitter_analytics',
              url = url_mongo,
              verbose = FALSE, options = ssl_options())
  df<-data_twitter$find('{"category":"price_distribution","state":"active"}')
  df2<-data_twitter$find('{"category":"influencer_distribution","state":"active"}')
  

  #General Target Prices
  output$twitter_target_price <-  renderPlot({
    constituent = toString(input$constituent_twitter)
      general_target_price_bar(df,constituent)
  })
  
  ##Influencer Target Prices
  output$influencer_target_price<-renderPlot({
    constituent = toString(input$constituent_twitter)
    influencer_target_price_bar(df2,constituent)
  })
  
  
  ##Top Mentions - Horizontal Bar Chart
  db_organization <- mongo(collection = 'twitter_top_organizations',
              url = url_mongo,
              verbose = FALSE, options = ssl_options())
  db_organization<-db_organization$find()
  output$organization <- renderPlot({
    constituent = toString(input$constituent_twitter)
    top_mentions_bar(db_organization,constituent)
    })
    
  
  #Twitter Sentiment Trend - Line Plot
  data_sent <- mongo(collection = 'twitter_sentiment_trend',
                     url = url_mongo,
                     verbose = FALSE, options = ssl_options())
  data_sent=data_sent$find('{}')
  
  output$sent_trend <-renderPlot({
    constituent = toString(input$constituent_twitter)    
    avg_twitter_sent(data_sent,constituent)
  })
  
  
  ##World Twitter Data  - Map Plot
  data <- mongo(collection = 'country_data',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
  country_df = data$find()
  
  ##Sentiment Mapping
  output$sentiment_map<-renderPlot({
    constituent = toString(input$constituent_twitter)
    map_sentiment(country_df,constituent)
     })
  
  ##Frequency Mapping
  output$popularity_map<-renderPlot({
    constituent = toString(input$constituent_twitter)
    map_frequency(country_df,constituent)
    })
  
  
  ########################################## News #########################################
  ##Top Words - World Cloud
  top_words_data <- mongo(collection = 'top_words',
                          url = url_mongo,
                          verbose = FALSE, options = ssl_options())
  top_words_df <- top_words_data$find()
  output$word_cloud <-renderPlot({
    constituent <-toString(input$constituent_news)
    word_cloud_plot(top_words_df,constituent)
  })
  
  ##News Tagging Count - Multicolored Vertical Bar Chart
  data <- mongo(collection = 'news_tag',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
  news_df = data$find()
  
  output$news_tag_bar<-renderPlot({
    constituent <-toString(input$constituent_news)
    count_tags_bar(news_df,constituent)
    })
  
  
  ##News Sentiment Trend - Line Graph
  output$news_sentiment_daily<-renderPlot({
    constituent <-toString(input$constituent_news)
    daily_news_sent(url_mongo,constituent)
  })
  
  
  ##News Sentiment Trend by Categories - Line Graph
  news_tag <- mongo(collection = 'news_tagging_score',
                    url = url_mongo,
                    verbose = FALSE, options = ssl_options())
  news_tag_df<-news_tag$find('{}')
  
  ##Average daily sentiment by category
  output$news_sentiment_tag<-renderPlot({
    constituent <-toString(input$constituent_news)
    news_sent_by_tag(news_tag_df,constituent)
    })
  
  
  ################################# Correlation Page ######################################
  ### News Sentiment Line 
  
  output$news_behavior_line <- renderPlotly({
    constituent = toString(input$constituent_corr)
    correlation_news(url_mongo,constituent)
  })
  
  
  ## Twitter Sentiment line
  output$twitter_behavior_line <- renderPlotly({
    constituent = toString(input$constituent_corr)
    correlation_twitter(url_mongo,constituent)
  })
  
  
  ### Add explanations
  output$news_annotation<- renderText({
    constituent = toString(input$constituent_corr)
    news_annotation_selection(constituent)
  })
  
  
  output$twitter_annotation<- renderText({
    constituent = toString(input$constituent_corr)
    twitter_annotation_selection(constituent)
    })
  
}

 

