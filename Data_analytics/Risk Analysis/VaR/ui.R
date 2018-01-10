library(shiny)
fluidPage(
  h1("Get the VaR from your portfolio - Demo"),
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
              value = "2017-01-01", min = NULL, max = NULL, format = "dd-mm-yyyy", startview = "month", weekstart = 1, language = "en", width = '100%'),
  textInput(inputId = "ticker",
            label = "Pick the stock of your choice(accept stock ticker only)",
            value = "TSLA"),
  plotOutput("bar")
)