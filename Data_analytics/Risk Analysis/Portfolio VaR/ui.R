library(quantmod)
library(PerformanceAnalytics)
library(reshape2)
library(ggplot2)
library(shiny)

fluidPage(
  
  # Application title
  titlePanel("Value at Risk of your Portfolio"),
  
  # Date Input
  dateInput(inputId = "startdate",
            label = "Pick your start date",
            value = "2016-01-01", min = NULL, max = NULL, format = "dd-mm-yyyy", startview = "month", weekstart = 1, language = "en", width = '100%'),
  
  selectInput(inputId = "portfolio",
              label = "Build your portfolio",
              choices = c("Allianz" = "ALIZF",
                          #"BMW" = "BMW",
                          "Deutsche Bank" = "DB",
                          "SAP" = "SAP",
                          #"Siemens" = "SIE",
                          #"Volkswagen" = "VOW3",
                          "Tesla" = "TSLA",
                          "Twitter" = "TWTR",
                          "Facebook" = "FB"),
              selected = c("Tesla" = "TSLA",
                           "Twitter" = "TWTR",
                           "Facebook" = "FB"),
              multiple = TRUE,
              selectize = TRUE, width = NULL, size = NULL),
  p(style = "font-family:Verdana",
    "Please make sure that there are at least two assets in your portfolio"
  ),
  
  selectInput(inputId = "weights",
              label = "Select the weight of each asset in your porfolio accordingly",
              choices = seq(from = 0, to = 1, by = 0.01),
              selected = c("20%" = 0.20,
                           "30%" = 0.30,
                           "50%" = 0.50), 
              multiple = TRUE,
              selectize = TRUE, width = NULL, size = NULL),
  p(style = "font-family:Verdana",
    "The sum of weights must be 100%, you can just type the number and select the number you want to put in"
  ),
  p(style = "font-family:Verdana",
    "Please note that the weights in your portfolio will only affect the value of Portfolio VaR"
  ),
  
  submitButton(text = "Submit your own portfolio", icon = NULL, width = NULL),

  #textOutput("portfolio_name")
  plotOutput("portfolio_name")
)
