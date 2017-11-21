library(shiny)
library(shinydashboard)
library(ggplot2)
library(readr)
library(plotly)
library(shinythemes)


ui <- dashboardPage(
  dashboardHeader( 
    title = 'iGenie Analytics'),
  dashboardSidebar(
    sidebarMenu(
      actionButton(inputId = "reload", label = "Refresh",icon=icon('refresh'),width='85%'),
      menuItem("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
      menuItem("Twitter Sentiment", tabName = "twitter_analysis", icon = icon("th")),
      menuItem("News Sentiment", tabName = "news_analysis", icon = icon("th")),
      menuItem("Price vs. Sentiment", tabName = "correlation", icon = icon("th")),
      menuItem("Market & Analysts", tabName = "analyst_prediction", icon = icon("th")),
      menuItem("Fundamental", tabName = "fundamental_analysis", icon = icon("th"))
    )
  ),
  
  dashboardBody(
    tabItems(
      ############################ HOMEPAGE ####################################
      tabItem(tabName='dashboard',
            fluidRow(
              tabBox(title = 'Twitter',
                     side='right',
                     id = 'tabset1',height='550px',
                     tabPanel('EON',
                              plotOutput("tweet_num_eon", height = 420)),
                     tabPanel('Deutsche Bank',
                              plotOutput("tweet_num_db", height = 420)),
                     tabPanel('Commerzbank',
                              plotOutput("tweet_num_cb", height = 420)),
                     tabPanel('BMW',
                              plotOutput("tweet_num_bmw", height = 420)),
                     tabPanel('Adidas',
                              plotOutput("tweet_num_adidas", height=420))
                     ),
              
              ##implement some sort of auto-height
              tabBox( title = 'News',
                      side='right',
                      id='tabset1',height='550px',
                      tabPanel('All',
                               DT::dataTableOutput('news_all'))
                      
                      )
              ),
      
            
            fluidRow(
             tabBox(
                #display analyst recommendation, and perhaps target prices
                title = "Analyst Prediction",
                id='tabset1',
                side='right',
                height=400,
                #status = 'primary',
                tabPanel('All',
                plotOutput("analystplot", height = 280, width = '100%')
                )
              ),
              
              tabBox(
                #display the color coded box
                height=400,
                title = "Summary",
                side='right',
                #status = 'primary',
                tabPanel('All',
                DT::dataTableOutput('reactivetable'))
                
              )
            )
      ),
      
      ############################ FUNDAMENTAL PAGE ####################################
      tabItem(tabName = 'fundamental_analysis',
              fluidRow(
                
                box(title='Profitability Ranking',
                    height=600,
                    align='center',
                    DT:: dataTableOutput('ranking_top')
                ),
                
                
                box(title='Cumulative Return',
                    height=600,
                    align='center',
                    #status = 'primary',
                    DT::dataTableOutput('CRtable')
                )),
              
              fluidRow(
                box(title= 'Earnings per Share',
                    height=400,
                    align='center',
                    DT::dataTableOutput('EPS_table')
                ),
                
                
                box(title='Price/Earning Ratio',
                    height=400,
                    align='center',
                    DT::dataTableOutput('PER_table')    
                ))
      ),
      
      ############################ ANALYST PAGE ####################################
      tabItem(tabName = 'analyst_prediction',
        fluidRow(
          box(title= 'Analyst Recommendation',
              height=600,
              align='center',
              DT::dataTableOutput('recommendation_table') 
                ),
          box(title='Target Prices',
              height=600,
              align='center',
              DT::dataTableOutput('target_price_table') 
                )
          )
         
          
        ),
      
      
      ############################ NEWS PAGE ####################################
      tabItem(tabName = 'news_analysis',
              selectInput('constituent_news', "Select constituent", c('Adidas','BMW','Deutsche Bank','Commerzbank','EON'), selected = 'Adidas', multiple = FALSE,
                          selectize = TRUE, width = NULL, size = NULL),
              fluidRow(
                box(title= 'News Tagging',
                    height=500,
                    plotOutput('news_tag_bar') 
                ),
                box(title='News Top Words',
                    height=500,
                    plotOutput('word_cloud') 
                )
              ),
              
              #news_sentiment_tag
              fluidRow(
                box(title='News Sentiment Trend',
                  height=500,
                  plotOutput('news_sentiment_daily',height=400)
               ),
              
               box(title='News Sentiment Analysis by Category',
                  height=500,
                  plotOutput('news_sentiment_tag',height=400)
              )
              )
              ),
      
      ############################ CORRELATION PAGE ####################################
      tabItem(tabName = 'correlation',
              selectInput('constituent_corr', "Select constituent", c('Adidas','BMW','Deutsche Bank','Commerzbank','EON'), selected = 'adidas', multiple = FALSE,
                          selectize = TRUE, width = NULL, size = NULL),
              fluidRow(
                
                box(title='News Sentiment vs. Stock Price Behavior',
                  height=620,
                   plotlyOutput('news_behavior_line',height="80%"),
                    h6("   "),
                   textOutput('news_annotation')
                   ),
          
                box(title='Twitter Sentiment vs. Stock Price Behavior',
                    height=620,
                    plotlyOutput('twitter_behavior_line',height="80%"),
                    h6("    "),
                    textOutput('twitter_annotation')
                  )
              )
              
            ),
      
      ############################ TWITTER PAGE ####################################
      tabItem(tabName = 'twitter_analysis',
              selectInput('constituent_twitter', "Select constituent", c('Adidas','BMW','Deutsche Bank','Commerzbank','EON'), selected = 'Adidas', multiple = FALSE,
                          selectize = TRUE, width = NULL, size = NULL),
              fluidRow(
                tabBox(title=h4("Twitter Target Price"), height=500,side='right',
                    tabPanel('General',
                             plotOutput("twitter_target_price", height=420)),
                
                    tabPanel('Influencer',
                             plotOutput("influencer_target_price", height=420))),
                
                tabBox(title = h4('Geographical Analysis'),height=500,side='right',
                       tabPanel('Sentiment',
                                plotOutput('sentiment_map',height=400)),
                       tabPanel('Frequency',
                                plotOutput('popularity_map',height=400)))),
                
                
              fluidRow(  
              box(title='Twitter Sentiment Trend',
                  align='center',
                  plotOutput('sent_trend',height=400),
                  height=500),
              
              box(title='Relevant Organizations',
                  align='center',
                  plotOutput('organization',height=400),
                  height=500))
         )
      )
    )
)




