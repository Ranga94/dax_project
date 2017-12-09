library(wordcloud)
library(RColorBrewer)
library(plotly)
library(treemap)


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
  
  #set_service_token("/home/ulysses/dax_project/keys/igenie-project-key.json")
  #set_service_token("/home/ulysses/dax_project/Dashboard_R_Big_Query/service_key/igenie-project-key.json")
  set_service_token("igenie-project-key.json")
  project <- "igenie-project" 
  
  ######################################  Main content of the dashboard  ##############################################
  
  ##############################    HOMEPAGE    ###################################
  
  ##Popular tweets
  
  #Treemap
  output$popular_treemap<-renderPlot({
    tweet_popular_df<-list_tabledata('igenie-project',"pecten_dataset","twitter_sentiment_popularity")
    popular_tweet_treemap(tweet_popular_df)
  })
  
  
  ## News Section
  ## News Table - Interactive DataTable
  from_date <- as.integer(as.POSIXct(strptime("2017-11-10","%Y-%m-%d"))) * 1000
  to_date <- as.integer(as.POSIXct(strptime("2017-11-16","%Y-%m-%d"))) * 1000
  
  query <- paste0('{"NEWS_DATE_NewsDim":{"$gte":{"$date":{"$numberLong":"', from_date,'"}},
                  "$lte":{"$date":{"$numberLong":"', to_date,'"}}},
                  "constituent_id":{"$exists":true} }')
  
  news_data_all <- eventReactive(input$reload, {
    sql <- 'SELECT constituent,NEWS_TITLE_NewsDim,NEWS_DATE_NewsDim,categorised_tag,sentiment FROM[igenie-project:pecten_dataset.news_all];'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    news_transform(retrieved_data)
    
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
  
  
  #analyst_opinions$Date<-as.Date(analyst_opinions$Date)
  #retrieved_data<-retrieved_data[retrieved_data$Date==as.Date('2017-11-21'),]
  #sql <- 'SELECT * FROM[igenie-project:pecten_dataset.analyst_opinions];'
  
  
  sql <- 'SELECT * FROM[igenie-project:pecten_dataset.analyst_opinions];'
  analyst_opinions <- query_exec(project=project,  sql, billing = project)
  analyst_opinions$Date<-as.Date(analyst_opinions$Date)
  analyst_opinions<-analyst_opinions[analyst_opinions$Date==as.Date('2017-11-21'),]
  
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
  
  sql <- 'SELECT * FROM[igenie-project:pecten_dataset.Summary_box];'
  retrieved_summary_data <- query_exec(project=project,  sql, billing = project)
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
  
  ##Cumulative Returns - DataTable
  output$CRtable  <- renderDataTable({
    sql <- 'SELECT * FROM [igenie-project:pecten_dataset.price_analysis] WHERE Status ="active";'
    #sql <- 'SELECT * FROM [igenie-project:pecten_dataset.price_analysis] WHERE Table ="cumulative returns analysis";'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    cumulative_returns<-retrieved_data[retrieved_data$Table=='cumulative return analysis',]
    cumulative_returns$Date<-as.Date(cumulative_returns$Date)
    cumulative_returns= cumulative_returns[cumulative_returns$Date==as.Date('2017-11-22'),]
    cumulative_return_table(cumulative_returns)
  })
  
  
  ##Profitability Ranking - DataTable
  output$ranking_top  <- renderDataTable({
    sql <- 'SELECT * FROM[igenie-project:pecten_dataset.profitability_ranking] WHERE Status = "active";'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    retrieved_data$Date<-as.Date(retrieved_data$Date)
    #retrieved_data=retrieved_data[retrieved_data$Date==as.Date('2017-10-04'),]
    rank_n_tag(retrieved_data)
  })
  
  ##Retrieve the fundamental data
  sql <- 'SELECT * FROM[igenie-project:pecten_dataset.fundamental_analysis] WHERE Status ="active";'
  fundamental_data <- query_exec(project=project,  sql, billing = project)
  
  
  ##EPS Analysis - DataTable
  output$EPS_table <- renderDataTable({
    EPS_data<-fundamental_data[fundamental_data$Table == 'EPS analysis',]
    EPS_table(EPS_data)
  })
  
  ##PER Analysis - DataTable
  output$PER_table <- renderDataTable({
    PER_data<-fundamental_data[fundamental_data$Table == 'PER analysis',]
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
  
  
  ##############################  TWITTER PAGE #######################################
  output$general_twitter_target_price<-renderPlot({
    sql <- 'SELECT * FROM[igenie-project:pecten_dataset.target_prices];'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    constituent = toString(input$constituent)
    general_target_price_bar(retrieved_data,constituent)
  })
  
  #Deutsche Borse, bmw, henkel, infenion,Volkswagen shows error
  output$influencer_twitter_target_price<-renderPlot({
    sql <- 'SELECT * FROM[igenie-project:pecten_dataset.influencer_prices];'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    constituent = toString(input$constituent)
    influencer_target_price_bar(retrieved_data,constituent)
  })
  
  
  output$tweet_num <-renderPlot({
    sql <- 'SELECT * FROM[igenie-project:pecten_dataset.twitter_sentiment_count_daily];'
    twitter_counts <- query_exec(project=project,  sql, billing = project)
    twitter_counts$date<-as.Date(twitter_counts$date)
    twitter_counts<- twitter_counts[twitter_counts$date>as.Date('2017-10-01'),]
    twitter_counts<- twitter_counts[twitter_counts$date<as.Date('2017-11-16'),]
    twitter_counts<- unique(twitter_counts)
    constituent = toString(input$constituent)
    tweet_count(twitter_counts, constituent)
  })
  
  
  ##World Twitter Data  - Map Plot
  ##Prosiebensat1, Vonovia, Volkswagen shows error
  ##Sentiment Mapping
  ##Need country_data3
  output$sentiment_map<-renderPlot({
    sql <- 'SELECT * FROM[igenie-project:pecten_dataset.country_data];'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    retrieved_data$date_of_analysis <- as.Date(retrieved_data$date_of_analysis )
    #country_df<- country_df[country_df$`date of analysis` == as.Date('2017-11-26'),]
    constituent = toString(input$constituent)
    map_sentiment(retrieved_data,constituent)
  })
  
  ##Frequency Mapping
  output$popularity_map<-renderPlot({
    sql <- 'SELECT * FROM[igenie-project:pecten_dataset.country_data];'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    retrieved_data$date_of_analysis <- as.Date(retrieved_data$date_of_analysis )
    #country_df<- country_df[country_df$`date of analysis` == as.Date('2017-11-26'),]
    constituent = toString(input$constituent)
    map_frequency(retrieved_data,constituent)
  })
  
  
  ##Most Recent Tweets
  output$recent_tweets_table <- renderDataTable({
    sql <- 'SELECT * FROM[igenie-project:pecten_dataset.twitter_analytics_latest_price_tweets];'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    retrieved_data[retrieved_data$constituent=='adidas',c('constituent')] = 'Adidas'
    constituent = toString(input$constituent)
    recent_tweets(retrieved_data,constituent)
  })
  
  
  ########################################## News #########################################
  
  ##News Tagging Count - Multicolored Vertical Bar Chart
  output$news_tag_bar<-renderPlot({
    sql <- 'SELECT * FROM[igenie-project:pecten_dataset.news_tag];'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    #tag_count_df$date <-as.Date(tag_count_df$date)
    #tag_count_df <-tag_count_df[tag_count_df$tags!='None',]
    #tag_count_df <-tag_count_df[tag_count_df$date > as.Date('2017-11-06'),]
    constituent <-toString(input$constituent)
    count_tags_bar(retrieved_data ,constituent)
  })
  
  
  ##News Sentiment Trend - Line Graph
  output$news_sentiment_daily<-renderPlot({
    sql <- 'SELECT * FROM[igenie-project:pecten_dataset.news_analytics_daily_sentiment];'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    constituent <-toString(input$constituent)
    daily_news_sent(retrieved_data,constituent)
  })
  
  
  ##News sentiment by Category - Heatmap
  output$topic_sentiment_grid<-renderPlot({
    sql <- 'SELECT * FROM[igenie-project:pecten_dataset.news_analytics_topic_sentiment];'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    constituent <-toString(input$constituent)
    topic_sentiment_grid_plot(retrieved_data,constituent)
  })
  
  
  #News articles by topic
  #query <- paste0('{}')
  #news_analytics_topic_articles_df <- news_analytics_topic_articles_conn$find(query)
  
  news_analytics_topic_articles_all <- eventReactive(input$constituent, {
    sql <- 'SELECT NEWS_DATE_NewsDim,constituent,categorised_tag,NEWS_TITLE_NewsDim, NEWS_ARTICLE_TXT_NewsDim FROM[igenie-project:pecten_dataset.news_analytics_topic_articles];'
    retrieved_data <- query_exec(project=project,  sql, billing = project)
    constituent = toString(input$constituent)
    constituent = 'Allianz'
    news_analytics_topic_articles_func(retrieved_data,constituent)
  }, ignoreNULL = FALSE)
  
  output$news_analytics_topic_articles <- DT::renderDataTable(news_analytics_topic_articles_all(),
                                                              selection = 'single', server=FALSE)
  
  observeEvent(input$news_analytics_topic_articles_rows_selected,
               {
                 i = input$news_analytics_topic_articles_rows_selected
                 cat(i)
                 i <- i[length(i)]
                 constituent = toString(input$constituent)
                 df<-retrieved_data[retrieved_data$constituent == constituent,]
                 showModal(modalDialog(
                   title = df[i,c('NEWS_TITLE_NewsDim')],
                   df[i,c('NEWS_ARTICLE_TXT_NewsDim')]
                 ))
               })
  
  
  
  
  ################################# Correlation Page ######################################
  sql <- 'SELECT * FROM[igenie-project:pecten_dataset.all_correlations];'
  correlation_data <- query_exec(project=project,  sql, billing = project)
  
  
  ## News Sentiment line
  output$news_behavior_line <- renderPlotly({
    constituent = toString(input$constituent)
    correlation_data<-correlation_data[correlation_data$Constituent==constituent,]
    correlation_news(correlation_data,constituent)
  })
  
  
  ## Twitter Sentiment line
  output$twitter_behavior_line <- renderPlotly({
    constituent = toString(input$constituent)
    correlation_data<-correlation_data[correlation_data$Constituent==constituent,]
    correlation_twitter(correlation_data,constituent)
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