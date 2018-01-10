library(quantmod)
library(PerformanceAnalytics)
library(reshape2)
library(ggplot2)

#step1 get the input ready
maxDate <- "2016-01-01"
tickers <- c("TSLA")#,"TWTR","FB")
weights <- c(0.20)#,0.30,0.50)
n <- length(tickers)
  
#step2 get the specific data required for calculation

if (n == 1) {
  getSymbols(tickers, auto.assign = F, from = maxDate)
} else {
  getSymbols(tickers, from = maxDate, auto.assign=TRUE)
}
  #take the adjusted price column into one dataframe
  #My first attempt!!! 
    #Port.prices <- merge(Ad(TSLA),Ad(AAPL),Ad(AMZN))       #??????this step, how to put merge(tickers) instead of put name into the fuction.
  #My Second attenpt!!! 
    #Port.prices <- merge(Ad(eval(parse(text = tickers[1]))),Ad(eval(parse(text = tickers[2]))),Ad(eval(parse(text = tickers[3]))))

Port.prices <- data.frame()
Port.prices <- Ad(eval(parse(text = tickers[1])))
for (i in c(2:n)) { 
  #Port.prices$tickers[i] <- Ad(eval(parse(text = tickers[i])))
  Port.prices <- cbind(Port.prices ,Ad(eval(parse(text = tickers[i]))))
}

  #then calculate the returns
Port.returns <- ROC(Port.prices, type="discrete")[-1,]

  #change the column name for better outlook
colnames(Port.returns) <- tickers

#step3 calculate the VaR in our porfolio
VaR.Hist <- VaR(Port.returns, p=0.95, weights = NULL, portfolio_method = "single", method = "historical")
VaR.Gaus <- VaR(Port.returns, p=0.95, weights = NULL, portfolio_method = "single", method = "gaussian")
VaR.Mod <- VaR(Port.returns, p=0.95, weights = NULL, portfolio_method = "single", method = "modified")

All.VAR <- data.frame(rbind(VaR.Hist, VaR.Gaus, VaR.Mod))
rownames(All.VAR) <- c("Hist", "Gaussian", "Modified")

VaR.Port.Hist <- VaR(Port.returns, p=0.95, weights = weights, portfolio_method = "component", method = "historical")
VaR.Port.Gaus <- VaR(Port.returns, p=0.95, weights = weights, portfolio_method = "component", method = "gaussian")$VaR[1,1]
VaR.Port.Mod <- VaR(Port.returns, p=0.95, weights = weights, portfolio_method = "component", method = "modified")$MVaR[1,1]

All.VAR$Portfolio <- c(VaR.Port.Hist, VaR.Port.Gaus, VaR.Port.Mod)
All.VAR <- abs(All.VAR)
All.VAR$Type <- c("Historical", "Gaussian", "Modified")

#final step plot!
plotVar <- melt(All.VAR, variable.name = "tickers", value.name = "VaR")
ggplot(plotVar, aes(x=Type, y=VaR, fill=tickers)) + geom_bar(stat = "identity", position = "dodge")
