##Stores all the functions used for the Fundamental Page of Analytics Dashboard. 
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

##This function creates a color-coded cumulative return datatable
cumulative_return_table<-function(retrieved_data){
df<-retrieved_data[,c('Constituent','6 months return','1 year return','3 years return')]
df<-df[order(df$Constituent),]
df[df$Constituent=='adidas',c('Constituent')]<-'Adidas'
df[,c('6 months return','1 year return','3 years return')]<-round(df[,c('6 months return','1 year return','3 years return')],2)
datatable(df,rownames = FALSE,options = list(pageLength = 10),colnames = c('Constituent','6 months cml.return','1 year cml.return','3 years cml.return')) %>%
  formatStyle(c('6 months return','1 year return','3 years return'),
              backgroundColor = styleInterval(c(0,0.5),c('red','white','#1E8449')))}

##This function creates a datatable for the crossing status
cross_analysis<-function(retrieved_data){
  df<-retrieved_data[,c('Constituent','Recent cross','Status of SMA 50')]
  df<-df[order(df$Constituent),]
  df[df$Constituent=='adidas',c('Constituent')]<-'Adidas'
  datatable(df,rownames = FALSE,options = list(pageLength = 5),colnames = c('Constituent','Recent cross','SMA 50 movement')) 
}

#This function creates a datatable for EPS
EPS_table<-function(retrieved_data){
  df<-retrieved_data[,c('Constituent','Current EPS','EPS last year')]
  df[df$Constituent=='adidas',c('Constituent')]<-'Adidas'
  df<-df[order(df$Constituent),]
  datatable(df,rownames = FALSE,options = list(pageLength = 5), colnames = c('Constituent','Current EPS value','EPS value last year'))%>% formatStyle(
    'Current EPS',
    background = styleColorBar(df$`Current PER`, 'orange'),
    backgroundSize = '100% 90%',
    backgroundRepeat = 'no-repeat',
    backgroundPosition = 'center') %>%formatStyle(
      'EPS last year',
      background = styleColorBar(df$`PER last year`, '#62B5F6'),
      backgroundSize = '100% 90%',
      backgroundRepeat = 'no-repeat',
      backgroundPosition = 'center')}


#This function creates a datatable for Profitability Ranking and Tags
rank_n_tag<-function(retrieved_data){
  df<-retrieved_data[,c('Profitability rank','Constituent','Price growth','Fundamental growth')]
  df$'Profitability rank'<- df$'Profitability rank'+1
  datatable(df,rownames = FALSE,options = list(pageLength = 10),colnames = c('Rank','Constituent','Growth in stock price','Growth in fundamental')) %>%formatStyle('Profitability rank', textAlign = 'center')
}