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
  url_mongo <- "mongodb://igenie_readwrite:igenie@35.197.245.249:27017/dax_gcp"
  
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
  ## News Table - Interactive DataTable
  from_date <- as.integer(as.POSIXct(strptime("2017-11-10","%Y-%m-%d"))) * 1000
  to_date <- as.integer(as.POSIXct(strptime("2017-11-16","%Y-%m-%d"))) * 1000
  
  query <- paste0('{"NEWS_DATE_NewsDim":{"$gte":{"$date":{"$numberLong":"', from_date,'"}},
                  "$lte":{"$date":{"$numberLong":"', to_date,'"}}},
                  "constituent_id":{"$exists":true} }')
  
  all_news <- mongo(collection = 'all_news',
                    url = url_mongo,
                    verbose = FALSE, options = ssl_options())
  db <- all_news$find(query)
  
  news_data_all <- eventReactive(input$reload, {
    news_transform(db)
  }, ignoreNULL = FALSE)
  
  output$news_all <- DT::renderDataTable(news_data_all(),selection = 'single', server=FALSE)
  
  observeEvent(input$news_all_rows_selected,
               {
                 i = input$news_all_rows_selected
                 i <- i[length(i)]
                 cat(i)
                 showModal(modalDialog(
                   title = db[i,c('NEWS_TITLE_NewsDim')],
                   db[i,c('NEWS_ARTICLE_TXT_NewsDim')]
                 ))
               })
  
  
  
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
  price_analysis_db <- mongo(collection = 'price_analysis',
                             url = url_mongo,
                             verbose = FALSE, options = ssl_options())
  
  fundamental_analysis_db<-mongo(collection = 'fundamental_analysis',
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
  
  
  general_target_prices_db<- mongo(collection = 'target_prices',
                        url = url_mongo,
                        verbose = FALSE, options = ssl_options())
  general_target_prices_df<- general_target_prices_db$find()
  output$general_twitter_target_price<-renderPlot({
    constituent = toString(input$constituent)
    general_target_price_bar(general_target_prices_df,constituent)
  })
  
  #Deutsche Borse, bmw, henkel, infenion,Volkswagen shows error
  influencer_target_prices_db<-mongo(collection = 'influencer_prices',
                                     url = url_mongo,
                                     verbose = FALSE, options = ssl_options())
  influencer_target_prices_df<- influencer_target_prices_db$find()
  output$influencer_twitter_target_price<-renderPlot({
    constituent = toString(input$constituent)
    influencer_target_price_bar(influencer_target_prices_df,constituent)
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
  ##Prosiebensat1, Vonovia, Volkswagen shows error
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
  
  
  ##Most Recent Tweets
  tweets_db <- mongo(collection = 'twitter_analytics_latest_price_tweets',
                url = url_mongo,
                verbose = FALSE, options = ssl_options())
  retrieved_data <- tweets_db$find('{}')
  retrieved_data[retrieved_data$constituent=='adidas',c('constituent')] = 'Adidas'
  output$recent_tweets_table <- renderDataTable({
    constituent = toString(input$constituent)
    recent_tweets(retrieved_data,constituent)
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
  
  
  ##News sentiment by Category - Heatmap
  topic_sentiment_db <- mongo(collection = 'news_analytics_topic_sentiment',
                    url = url_mongo,
                    verbose = FALSE, options = ssl_options())
  topic_sentiment_df<- topic_sentiment_db$find()
  
  output$topic_sentiment_grid<-renderPlot({
    constituent <-toString(input$constituent)
    topic_sentiment_grid_plot(topic_sentiment_df,constituent)
  })
  
  
  #News articles by topic
  query <- paste0('{}')
  
  news_analytics_topic_articles_conn <- mongo(collection = 'news_analytics_topic_articles',
                                              url = url_mongo,
                                              verbose = FALSE, options = ssl_options())
  news_analytics_topic_articles_df <- news_analytics_topic_articles_conn$find(query)
  
  news_analytics_topic_articles_all <- eventReactive(input$constituent, {
    constituent = toString(input$constituent)
    news_analytics_topic_articles_func(news_analytics_topic_articles_df,constituent)
  }, ignoreNULL = FALSE)
  
  output$news_analytics_topic_articles <- DT::renderDataTable(news_analytics_topic_articles_all(),
                                                              selection = 'single', server=FALSE)
  
  observeEvent(input$news_analytics_topic_articles_rows_selected,
               {
                 i = input$news_analytics_topic_articles_rows_selected
                 cat(i)
                 i <- i[length(i)]
                 constituent = toString(input$constituent)
                 df<-news_analytics_topic_articles_df[news_analytics_topic_articles_df$constituent == constituent,]
                 showModal(modalDialog(
                   title = df[i,c('NEWS_TITLE_NewsDim')],
                   df[i,c('NEWS_ARTICLE_TXT_NewsDim')]
                 ))
               })
  
  
  

  ################################# Correlation Page ######################################
  all_correlations<-mongo(collection = 'all_correlations',
                          url = url_mongo,
                          verbose = FALSE, options = ssl_options())
  all_correlations<- all_correlations$find()
  
  ## News Sentiment line
  output$news_behavior_line <- renderPlotly({
    constituent = toString(input$constituent)
    correlation_df<-all_correlations[all_correlations$Constituent==constituent,]
    correlation_news(correlation_df,constituent)
  })
  
  
  ## Twitter Sentiment line
  output$twitter_behavior_line <- renderPlotly({
    constituent = toString(input$constituent)
    correlation_df<-all_correlations[all_correlations$Constituent==constituent,]
    correlation_twitter(correlation_df,constituent)
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



