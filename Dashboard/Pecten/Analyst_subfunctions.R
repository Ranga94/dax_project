##This stores functions used for the Analysts Page of Analytics Dashboard.
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


analyst_rating_table<-function(retrieved_data){
  df<-retrieved_data[,c('Constituent','Analyst_recommendation','Analyst_rating')]
  df<-df[order(df$Constituent),]
  datatable(df,rownames = FALSE,options = list(pageLength = 10), colnames = c('Constituent','Recommendation','Rating')) %>% formatStyle(
    'Analyst_rating',
    background = styleColorBar(retrieved_data$`Analyst_rating`, 'steelblue'),
    backgroundSize = '100% 90%',
    backgroundRepeat = 'no-repeat',
    backgroundPosition = 'center')}


#Makes a table displaying target prices
analyst_target_prices<-function(retrieved_data){
  df<-retrieved_data[,c('Constituent','Median_target_price','Lowest_target_price','Highest_target_price')]
  df<-df[order(df$Constituent),]
  datatable(df,rownames = FALSE,options = list(pageLength = 10), colnames = c('Constituent','Median','Lowest','Highest'))  %>% formatCurrency(c('Median_target_price','Lowest_target_price','Highest_target_price'), '€')
}