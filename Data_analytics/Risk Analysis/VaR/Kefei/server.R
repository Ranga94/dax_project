library(shiny)
library(quantmod)
library(PerformanceAnalytics)
library(reshape2)
library(ggplot2)

server <- function(input, output){
  output$bar <- renderPlot({
    #the following code is the computation of VaRs
    maxDate <- str(input$startdate)
    ticker <- str(input$ticker)
    stock.prices <- Ad(getSymbols("TSLA", auto.assign = F, from = "2017-09-01"))
    stock.rtns <- dailyReturn(stock.prices)
    VAR.hist <- VaR(stock.rtns, p=0.99, method="historical")
    VAR.gaus <- VaR(stock.rtns, p=0.99, method="gaussian")
    VAR.mod <- VaR(stock.rtns, p=0.99, method="modified")
    ALL.VAR <- data.frame(rbind(VAR.hist,VAR.gaus,VAR.mod))
    rownames(ALL.VAR) <- c("Historical","Gaussian","Modified")
    colnames(ALL.VAR) <- ticker
    ALL.VAR <- abs(ALL.VAR)
    ALL.VAR$"Type" <- c("Historical","Gaussian","Modified")
    plotVar <- melt(ALL.VAR, variable.name = "ticker", value.name = "VaR")
    #computation of the VaRs stop here
    #next step I tried to plot it into a barchart which is ouput$bar.
    #Obviously it's just a failed attempt, I don't know how to finish the final step.
    
    ggplot(plotVar, aes(x=Type, y=VaR, fill=ticker)) + geom_bar(stat = "identity", position = "dodge")+labs(title = 'TSLA')
    #main = (input$ticker)
  })
}
