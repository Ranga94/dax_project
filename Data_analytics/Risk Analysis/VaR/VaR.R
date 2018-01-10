library(quantmod)
library(PerformanceAnalytics)
library(reshape2)
library(ggplot2)

maxDate <- "2017-09-01"
ticker <- "SIE.F"
stock.prices <- Ad(getSymbols(ticker, auto.assign = F, from = maxDate))
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
ggplot(plotVar, aes(x=Type, y=VaR, fill=ticker)) + geom_bar(stat = "identity", position = "dodge")
#x="VaR calculation method", y="VaR(Return loss)"