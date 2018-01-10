options(scipen=1000)
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
library(treemap)

server <- function(input, output){
  url_mongo <- "mongodb://igenie_readwrite:igenie@35.189.89.82:27017/dax_gcp"
  
  ######################################  Main content of the dashboard  ##############################################

  ##############################    HOMEPAGE    ###################################
  
  ##Popular tweets
  ##Bar chart
  popular_constituents_db <- mongo(collection = 'popular_constituents',
                             url = url_mongo,
                             verbose = FALSE, options = ssl_options())
  
  top_tweeted_constituents<-popular_constituents_db$find('{}')
  output$popular_constituents <- renderPlot(popular_constituents_bar(top_tweeted_constituents))
  
  
  #Treemap
  popular_count_sent <- mongo(collection = 'twitter_sentiment_popularity',
                                   url = url_mongo,
                                   verbose = FALSE, options = ssl_options())
  
  tweet_popular_df<-popular_count_sent$find('{}')
  output$popular_treemap<-renderPlot(popular_tweet_treemap(tweet_popular_df))
  
  ## News Section
  ## News Table - DataTable
  from_date <- as.integer(as.POSIXct(strptime("2017-11-10","%Y-%m-%d"))) * 1000
  to_date <- as.integer(as.POSIXct(strptime("2017-11-16","%Y-%m-%d"))) * 1000
  
  query <- paste0('{"NEWS_DATE_NewsDim":{"$gte":{"$date":{"$numberLong":"', from_date,'"}},
                  "$lte":{"$date":{"$numberLong":"', to_date,'"}}},
                  "constituent_id":{"$exists":true} }')
  
  proyection <- paste0('{"NEWS_ARTICLE_TXT_NewsDim":false}')
  

  output$news_all <- renderDataTable({
    all_news <- mongo(collection = 'all_news',
                        url = url_mongo,
                        verbose = FALSE, options = ssl_options())
    db <- all_news$find(query, proyection)
    news_transform(db)})
  
  
  analyst_opinions_db <- mongo(collection = 'analyst_opinions_all',
              url = url_mongo,
              verbose = FALSE, options = ssl_options())
  analyst_opinions <- analyst_opinions_db$find('{"Status":"active"}')
  analyst_opinions$Date<-as.Date(analyst_opinions$Date)
  #retrieved_data<-retrieved_data[retrieved_data$Date==as.Date('2017-11-21'),]
  
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
  summary_db <- mongo(collection = 'summary_box',
              url = url_mongo,
              verbose = FALSE, options = ssl_options())
  retrieved_summary_data <- summary_db$find('{}')
  retrieved_summary_data<-retrieved_summary_data[retrieved_summary_data$constituent!='DAX',]
  
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
  price_analysis_db <- mongo(collection = 'price analysis',
                             url = url_mongo,
                             verbose = FALSE, options = ssl_options())
  
  fundamental_analysis_db<-mongo(collection = 'fundamental analysis',
                                 url = url_mongo,
                                 verbose = FALSE, options = ssl_options())
  
  ##Cumulative Returns - DataTable
  output$CRtable  <- renderDataTable({
    retrieved_data <- price_analysis_db$find('{"Table":"cumulative return analysis","Status":"active"}')
    #Select wanted date
    #retrieved_data$Date<-as.Date(retrieved_data$Date)
    #retrieved_data=retrieved_data[retrieved_data$Date==as.Date('2017-10-08'),]
    cumulative_return_table(retrieved_data)
  })
  
  
  
  ##Profitability Ranking - DataTable
  output$ranking_top  <- renderDataTable({
    db <- mongo(collection = 'profitability_ranking',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
    retrieved_data<-db$find('{}')
    retrieved_data$Date<-as.Date(retrieved_data$Date)
    retrieved_data <- db$find('{"Status":"active"}')
    #retrieved_data=retrieved_data[retrieved_data$Date==as.Date('2017-10-04'),]
    rank_n_tag(retrieved_data)
  })
  
  
  ##EPS Analysis - DataTable
  output$EPS_table <- renderDataTable({
    retrieved_data <- fundamental_analysis_db$find('{"Table":"EPS analysis","Status":"active"}')
    EPS_table(retrieved_data)
  })
  
  
  ##PER Analysis - DataTable
  output$PER_table <- renderDataTable({
    retrieved_data <- fundamental_analysis_db$find('{"Table":"PER analysis","Status":"active"}')
    PER_table(retrieved_data)
  })
  
  
  ##################################### ANALYST PAGE ####################################
  analyst_opinions_all_db <- mongo(collection = 'analyst_opinions_all',
                                   url = url_mongo,
                                   verbose = FALSE, options = ssl_options())
  
  
  ##Analyst Recommendation - DataTable
  output$recommendation_table <- renderDataTable({
    retrieved_data <- analyst_opinions_all_db$find('{"Status":"active"}')
    analyst_rating_table(retrieved_data)
  }) 
  
  
  ##Analyst Target Prices - DataTable
  output$target_price_table <- renderDataTable({
    retrieved_data <- analyst_opinions_all_db$find('{"Status":"active"}')
    analyst_target_prices(retrieved_data)
  })
  
  
  ##############################  TWITTER PAGE #######################################
  ##Twitter Target Price Distribution - Vertical Bar Charts 
  data_twitter <- mongo(collection = 'twitter_analytics',
                        url = url_mongo,
                        verbose = FALSE, options = ssl_options())
  df<-data_twitter$find('{"category":"price_distribution","state":"active"}')
  df2<-data_twitter$find('{"category":"influencer_distribution","state":"active"}')
  
  
  #General Target Prices
  output$twitter_target_price <-  renderPlot({
    constituent = toString(input$constituent)
    general_target_price_bar(df,constituent)
  })
  
  ##Influencer Target Prices
  output$influencer_target_price<-renderPlot({
    constituent = toString(input$constituent)
    influencer_target_price_bar(df2,constituent)
  })
  
  
  ##Top Mentions - Horizontal Bar Chart
  db_organization <- mongo(collection = 'twitter_top_organizations',
                           url = url_mongo,
                           verbose = FALSE, options = ssl_options())
  db_organization<-db_organization$find()
  #db_organization<-db_organization$find('{"status":"active"}')
  db_organization<-db_organization[db_organization$`date of analysis`==as.Date('2017-11-20'),]
  output$organization <- renderPlot({
    constituent = toString(input$constituent)
    top_mentions_bar(db_organization,constituent)
  })
  
  
  #Twitter Sentiment Trend - Line Plot
  data_sent_all <- mongo(collection = 'twitter_sentiment_trend',
                         url = url_mongo,
                         verbose = FALSE, options = ssl_options())
  data_sent_all=data_sent_all$find('{}')
  
  output$sent_trend <-renderPlot({
    constituent = toString(input$constituent)    
    avg_twitter_sent(data_sent_all,constituent)
  })
  
  
  twitter_counts<- mongo(collection = 'twitter_sentiment_count_daily3',
                         url = url_mongo,
                         verbose = FALSE, options = ssl_options())
  twitter_counts=twitter_counts$find('{}')
  twitter_counts$date<-as.Date(twitter_counts$date)
  twitter_counts<- twitter_counts[twitter_counts$date>as.Date('2017-10-01'),]
  twitter_counts<- twitter_counts[twitter_counts$date<as.Date('2017-11-16'),]
  twitter_counts<- unique(twitter_counts)
  
  output$tweet_num<- tweet_count_adidas <-renderPlot({
    constituent = toString(input$constituent)
    tweet_count(twitter_counts, constituent)
  })
  
  
  
  ##World Twitter Data  - Map Plot
  data <- mongo(collection = 'country_data3',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
  country_df = data$find('{}')
  country_df$`date of analysis` <- as.Date(country_df$`date of analysis` )
  #country_df<- country_df[country_df$`date of analysis` == as.Date('2017-11-26'),]
  
  
  ##Sentiment Mapping
  output$sentiment_map<-renderPlot({
    constituent = toString(input$constituent)
    map_sentiment(country_df,constituent)
  })
  
  ##Frequency Mapping
  output$popularity_map<-renderPlot({
    constituent = toString(input$constituent)
    map_frequency(country_df,constituent)
  })
  
  
  ########################################## News #########################################
  ##Top Words - World Cloud
  top_words_data <- mongo(collection = 'top_words',
                          url = url_mongo,
                          verbose = FALSE, options = ssl_options())
  top_words_df <- top_words_data$find('{}')
  output$word_cloud <-renderPlot({
    constituent <-toString(input$constituent)
    word_cloud_plot(top_words_df,constituent)
  })
  
  ##News Tagging Count - Multicolored Vertical Bar Chart
  tag_count_data <- mongo(collection = 'news_tag',
                          url = url_mongo,
                          verbose = FALSE, options = ssl_options())
  tag_count_df = tag_count_data$find('{}')
  #tag_count_df$date <-as.Date(tag_count_df$date)
  #tag_count_df <-tag_count_df[tag_count_df$tags!='None',]
  #tag_count_df <-tag_count_df[tag_count_df$date > as.Date('2017-11-06'),]
  
  
  output$news_tag_bar<-renderPlot({
    constituent <-toString(input$constituent)
    count_tags_bar(tag_count_df ,constituent)
  })
  
  
  ##News Sentiment Trend - Line Graph
  output$news_sentiment_daily<-renderPlot({
    constituent <-toString(input$constituent)
    daily_news_sent(url_mongo,constituent)
  })
  
  
  ##News Sentiment Trend by Categories - Line Graph
  tag_score_db<- mongo(collection = 'news_sentiment_by_tag',
                   url = url_mongo,
                   verbose = FALSE, options = ssl_options())
  tag_score_df<-tag_score_db$find('{}')
  tag_score_df$date<-as.Date(tag_score_df$date)
  tag_score_df<-tag_score_df[tag_score_df$date > as.Date('2017-10-01'),]
  
  ##Average daily sentiment by category
  output$news_sentiment_tag<-renderPlot({
    constituent <-toString(input$constituent)
    news_sent_by_tag(tag_score_df,constituent)
  })
  
  ################################# Correlation Page ######################################
  
  ## News Sentiment line
  output$news_behavior_line <- renderPlotly({
    constituent = toString(input$constituent)
    correlation_news(url_mongo,constituent)
  })
  
  
  ## Twitter Sentiment line
  output$twitter_behavior_line <- renderPlotly({
    constituent = toString(input$constituent)
    correlation_twitter(url_mongo,constituent)
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



