library(quantmod)
library(PerformanceAnalytics)
library(reshape2)
library(shiny)
library(bigrquery)

value_at_risk <- function(maxDate, tickers, weights, n, meth){
  set_service_token("igenie-project-key.json")
  #set_service_token("/Users/SparkingAries/OneDrive/Data_Science/iGenie Counsulting/igenie-project-key.json")
  project <- "igenie-project"
  port_prices <- data.frame()
  
  #get the first asset's price of selected date
  sql <- paste('SELECT closing_price,date FROM[igenie-project:pecten_dataset.historical] WHERE constituent="',tickers[1],'";',sep='')
  port_prices <- query_exec(project=project,  sql, billing = project)
  port_prices$date = as.Date(port_prices$date)
  port_prices <-port_prices[port_prices$date>as.Date(maxDate),]
  
  #for loop if it has more than one asset in the portfolio
  if (n == 1) {
    port_prices <- port_prices
  } else {
    for (i in c(2:n)) {
      sql <- paste('SELECT closing_price, date FROM[igenie-project:pecten_dataset.historical] WHERE constituent="',tickers[i],'";',sep='')
      prices <- query_exec(project=project,  sql, billing = project)
      prices$date = as.Date(prices$date)
      prices <-prices[prices$date>as.Date(maxDate),]
      port_prices <- merge(port_prices,prices,by="date")
    }
  }
  
  #build the dataframe of Rate of change in the portfolio
  #re-index
  rownames(port_prices) <- port_prices$date
  #delecte the colomn date
  port_prices$date <- NULL
  #calculate the rate of change of the prices
  port_returns <- ROC(port_prices, type="discrete")[-1,,drop = FALSE]
  #rename the column names
  colnames(port_returns) <- tickers
  
  # calculate the VaR of each assets in our porfolio
  All.VAR <- as.data.frame(VaR(port_returns, p=0.95, weights = NULL, portfolio_method = "single", method = meth))
  # VaR.Hist <- VaR(port_returns, p=0.95, weights = NULL, portfolio_method = "single", method = "historical")
  # VaR.Gaus <- VaR(port_returns, p=0.95, weights = NULL, portfolio_method = "single", method = "gaussian")
  # VaR.Mod <- VaR(port_returns, p=0.95, weights = NULL, portfolio_method = "single", method = "modified")
  #All.VAR <- data.frame(rbind(VaR.Hist, VaR.Gaus, VaR.Mod))
  #rownames(All.VAR) <- c("Hist", "Gaussian", "Modified")
  

  if (n == 1) {
    All.VAR <- abs(All.VAR)
    #All.VAR$Type <- c("Historical", "Gaussian", "Modified")
  } else {
    # calculate the VaR of the porfolio as a whole
    if (meth == 'historical'){
      Portfolio <- VaR(port_returns, p=0.95, weights = weights, portfolio_method = "component", method = meth)
      All.VAR <- cbind(All.VAR, Portfolio)
    } else if (meth == 'gaussian') {
      Portfolio <- as.numeric(VaR(port_returns, p=0.95, weights = weights, portfolio_method = "component", method = meth)$VaR[1,1])
      All.VAR <- cbind(All.VAR, Portfolio)
    } else 
      Portfolio <- VaR(port_returns, p=0.95, weights = weights, portfolio_method = "component", method = meth)$MVaR[1,1]
      All.VAR <- cbind(All.VAR, Portfolio)
  }
  All.VAR <- All.VAR[, !duplicated(colnames(All.VAR))]
  rownames(All.VAR) <- meth
  All.VAR <- abs(All.VAR)
  #All.VAR$Type <- meth
    
    # All.VAR$Portfolio <- VaR_port
    # All.VAR <- abs(All.VAR)
    # All.VAR$Type <- meth
    
    # VaR.Port.Hist <- VaR(port_returns, p=0.95, weights = weights, portfolio_method = "component", method = "historical")
    # VaR.Port.Gaus <- VaR(port_returns, p=0.95, weights = weights, portfolio_method = "component", method = "gaussian")$VaR[1,1]
    # VaR.Port.Mod <- VaR(port_returns, p=0.95, weights = weights, portfolio_method = "component", method = "modified")$MVaR[1,1]
    # 
    # All.VAR$Portfolio <- c(VaR.Port.Hist, VaR.Port.Gaus, VaR.Port.Mod)
    # All.VAR <- abs(All.VAR)
    # All.VAR$Type <- c("Historical", "Gaussian", "Modified")

  
  #final step plot!
  plotVar <- melt(All.VAR, variable.name = "Assets", value.name = "VaR")
  colnames(plotVar) <- c('Assets', 'VaR')
  #ggplot(plotVar, aes(x=Assets, y=VaR, fill=Assets)) + geom_bar(stat = "identity", position = "dodge") #+ scale_fill_manual(values = "Grey50", limits = 4)

  return(plotVar)
}


# maxDate <- '2017-01-01'
# tickers <- c('adidas','Allianz','BMW')
# weights <- c(1,0.3,0.3)
# n<-length(tickers)
# var_table <- value_at_risk(maxDate, tickers, weights, n)
# ggplot(var_table, aes(x=Type, y=VaR, fill=Assets)) + geom_bar(stat = "identity", position = "dodge")
