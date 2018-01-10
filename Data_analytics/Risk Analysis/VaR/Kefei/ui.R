library(shiny)
library(quantmod)
library(PerformanceAnalytics)
library(reshape2)
library(ggplot2)

ui<- ui <- dashboardPage(
  dashboardHeader( 
    title = 'Demo',
    dropdownMenu(
      type = "messages", 
      headerText = "random laughter",
      messageItem( from="Haha", message= "Hehehehehe",
                   icon = icon("exclamation-triangle"))
    )),
  ##Actually both Vonovia and ProSiebenSat1 dont have complete twitter and news data (country data + news sentiment)
  
  dashboardSidebar(
    sidebarMenu(
      #actionButton(inputId = "reload", label = "Refresh",icon=icon('refresh'),width='85%'),
      menuItem("Homepage", tabName = "dashboard", icon = icon("dashboard"))
    )
  ),
  
  dashboardBody(
    fluidPage(
      h1("Get the VaR in your portfolio - Demo"),
      p(style = "font-family:Verdana",
        "Take a look at my personal",
        a("Website",
          href = "https://harveyishe.wixsite.com/eastoceangvo")
      ),
      p(style = "font-family:Verdana",
        "In this app, the VaRs will be calculated in three methods, so you can have a comparison between them."
      ),
      p(style = "font-family:Verdana",
        "In the future update, you will be able to see the VaR in a porforlio with multiple assets."
      ),
      dateInput(inputId = "startdate",
                label = "Pick your start date",
                value = "yyyy-mm-dd", min = NULL, max = NULL, format = "dd-mm-yyyy", startview = "month", weekstart = 1, language = "en", width = '100%'),
      
      textInput(inputId = "ticker",
                label = "Pick the stock of your choice(accept stock ticker only)",
                value = "TSLA"),
      
      plotOutput("bar"),
      
      
      verbatimTextOutput("stats")
  )
))