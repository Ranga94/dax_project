library(shiny)
library(shinydashboard)
library(ggplot2)
library(readr)
library(plotly)
library(shinythemes)
library(bigrquery)

constituent_list = c('Adidas', 'Allianz', 'BASF', 'BMW', 'Bayer', 'Beiersdorf',
                     'Commerzbank', 'Continental', 'Daimler',
                     'Deutsche Bank', 'Deutsche Post',
                     'Deutsche Telekom', 'EON', 'Fresenius',
                     'Fresenius Medical Care', 'Infineon', 
                     'Lufthansa', 'Merck', 'SAP',
                     'Siemens')

ui <- dashboardPage(
  dashboardHeader( 
    title = 'iGenie Analytics',
    dropdownMenu(
      type = "messages", 
      #icon = icon("exclamation-triangle"),
      #badgeStatus = NULL,
      headerText = "Constituents that have incomplete data:",
      messageItem( from="Fundamental Analysis", message= "Henkel, Linde, Münchener RE",
                   icon = icon("exclamation-triangle")),
      messageItem(from = "Twitter Analysis", message= "Vonovia, Volkswagen, Deutsche Börse", icon = icon("exclamation-triangle")),
      messageItem(from ="News Analysis", message = "RWE, Thyssenkrupp, ProSiebenSat1", icon = icon("exclamation-triangle"))
    )),
  ##Actually both Vonovia and ProSiebenSat1 dont have complete twitter and news data (country data + news sentiment)
  
  
  dashboardSidebar(
    sidebarMenu(
      #actionButton(inputId = "reload", label = "Refresh",icon=icon('refresh'),width='85%'),
      menuItem("Homepage", tabName = "dashboard", icon = icon("dashboard")),
      menuItem("Market & Analysts", tabName = "analyst_prediction", icon = icon("th")),
      menuItem("Fundamental", tabName = "fundamental_analysis", icon = icon("th")),
      menuItem("Risk Analysis", tabName = "risk", icon = icon("th")),
      selectInput("constituent", "Select a constituent:", 
                  choices=constituent_list,
                  helpText("Select Constituent")),
      menuItem("Twitter Sentiment", tabName = "twitter_analysis", icon = icon("th")),
      menuItem("News Sentiment", tabName = "news_analysis", icon = icon("th")),
      menuItem("Price vs. Sentiment", tabName = "correlation", icon = icon("th"))
    )
  ),
  
  dashboardBody(
    tabItems(
      ############################ HOMEPAGE ####################################
      tabItem(tabName='dashboard',
              fluidRow(
                tabBox(title = 'Tweets',
                       side = 'right',
                       id = 'tabset1',height='550px',
                       tabPanel("All",
                                plotOutput('popular_treemap',height = '420px', width = '100%'))
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
                  title = "Analyst Recommendation",
                  id='tabset1',
                  side='right',
                  height=550,
                  selected = 'A-D',
                  #status = 'primary',
                  tabPanel('M-V',
                           plotOutput("analystplot3", height = 490, width = '100%')
                  ),
                  tabPanel('D-M',
                           plotOutput("analystplot2", height = 490, width = '100%')
                  ),
                  tabPanel('A-D',
                           plotOutput("analystplot1", height = 490, width = '100%')
                  )
                  
                ),
                
                tabBox(
                  #display the color coded box
                  height=550,
                  title = "Summary",
                  side='right',
                  selected = 'A-D',
                  #status = 'primary',
                  tabPanel('M-V',
                           DT::dataTableOutput('summarytable3')),
                  tabPanel('D-M',
                           DT::dataTableOutput('summarytable2')),
                  tabPanel('A-D',
                           DT::dataTableOutput('summarytable1'))
                  
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
                box(title= 'Earning per Share',
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
      
      
      
      ##################################### Risk Analysis Page ###########################################
      tabItem(tabName = 'risk',
              fluidPage(
                
                # Application title
                titlePanel("Value at Risk of your Portfolio"),
                
                mainPanel(
                  
                  h4("Instructions for use:"),
                  h5("Build your porfolio and get the VaR value (loss as percentage) of each asset and the portfolio as a whole in three different methods."),
                  h5("If there is just one asset selected, just ignore the weight input, submit to get result."),
                  
                  #Date Input
                  dateInput(inputId = "startdate",
                             label = "Pick your start date",
                             value = "2016-01-01", min = NULL, max = NULL, format = "dd-mm-yyyy", startview = "month", weekstart = 1, language = "en", width = 300),
                   
                   
                  selectInput(inputId = "portfolio",
                               label = "Select stocks to build your portfolio",
                               choices = c('adidas', 'Allianz', 'BASF', 'BMW', 'Bayer', 'Beiersdorf',
                                           'Commerzbank', 'Continental', 'Daimler',
                                           'Deutsche Bank', 'Deutsche Post',
                                           'Deutsche Telekom', 'EON', 'Fresenius',
                                           'Fresenius Medical Care', 'Infineon', 
                                           'Lufthansa', 'Merck', 'SAP',
                                           'Siemens'),
                               selected = c('adidas','Allianz','BMW'),
                               multiple = TRUE,
                               selectize = TRUE, width = NULL, size = NULL),
                   
                   
                  selectInput(inputId = "weights",
                               label = "Select the weight of each asset in your porfolio accordingly",
                               choices = seq(from = 0, to = 1.0, by = 0.01),
                               selected = c("20%" = 0.20,
                                            "30%" = 0.30,
                                            "50%" = 0.50), 
                               multiple = TRUE,
                               selectize = TRUE, width = NULL, size = NULL),
                  
                  selectInput(inputId = "meth",
                              label = "VaR valuation method",
                              choices = c("Historical" = "historical",
                                          "Gaussian" = "gaussian",
                                          "Modified" = "modified"),
                              selected = c("Historical" = "historical"), 
                              multiple = FALSE,
                              selectize = TRUE, width = NULL, size = NULL),
                  
                  h5(
                    "The sum of weights must be 100%, you can just type the number and select the number you want to put in"
                  ),
                  h5(
                    "Please note that the weights in your portfolio will only affect the value of Portfolio VaR."),
                  
                  #actionButton("submit", "Submit your portfolio"),
                  #submitButton(text = "Submit your portfolio", icon = NULL, width = NULL),
                  
                  #textOutput("portfolio_name"),
                  plotOutput("var_chart", width = "70%")
                #tableOutput('table')
              ))
      ),
      
      
      
      ############################ NEWS PAGE ####################################
      tabItem(tabName = 'news_analysis',
              fluidRow(
                box(title= 'News Category Count',
                    height=500,
                    plotOutput('news_tag_bar') 
                ),
                box(title='News Category Sentiment',
                    height=500,
                    plotOutput('topic_sentiment_grid') 
                )
              ),
              
              #news_sentiment_tag
              fluidRow(
                box(title='News Sentiment Trend',
                    height='500px',
                    plotOutput('news_sentiment_daily',height=450)
                ),
                
                box(title = 'News',
                    side='right',
                    id='tabset2',height='500px',
                    tabPanel('All',DT::dataTableOutput('news_analytics_topic_articles'))
                )
              )
      ),
      
      ############################ CORRELATION PAGE ####################################
      tabItem(tabName = 'correlation',
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
              
              fluidRow(
                 tabBox(title=h4("Twitter Target Price"), height=500,side='right',
                        tabPanel('General',
                                 plotOutput("general_twitter_target_price", height=420)),
                        
                        tabPanel('Influencer',
                                 plotOutput("influencer_twitter_target_price", height=420))),
                
                 tabBox(title='Recent Tweets',side = 'right',height=500,
                      tabPanel('Target Prices',
                        DT::dataTableOutput('recent_tweets_table'),
                        height=500)
                  )
              ),
                 
                 

              fluidRow(  
                 tabBox(title=h4('Twitter Sentiment Count'),height=500, side = 'right',
                    tabPanel("All Tweets",
                     #align='center',
                     plotOutput('tweet_num',height=400),
                     height=450)),
                 
                 tabBox(title = h4('Twitter Analysis by Countries'), side = 'right',
                        height=500,
                        tabPanel('Sentiment',
                                 plotOutput('sentiment_map',height=400)),
                        tabPanel('Frequency',
                                 plotOutput('popularity_map',height=400))
                 ))
                
                # box(title='Recent Tweets',
                #     align='center',
                #     DT::dataTableOutput('recent_tweets_table'),
                #     height=500))
      
          )
      )
    )
  )


