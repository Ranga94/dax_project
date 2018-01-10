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
  df<-retrieved_data[,c('Constituent','six_months_return','one_year_return','three_years_return')]
  df<-df[order(df$Constituent),]
  df[df$Constituent=='adidas',c('Constituent')]<-'Adidas'
  df[df$Constituent == 'Volkswagen (VW) vz',c('Constituent')]='Volkswagen'
  df[df$Constituent=='thyssenkrupp',c('Constituent')]<-'Thyssenkrupp'
  df[df$Constituent == 'Münchener Rückversicherungs-Gesellschaft',c('Constituent')]='Münchener RG'
  df[,c('six_months_return','one_year_return','three_years_return')]<-round(df[,c('six_months_return','one_year_return','three_years_return')],2)
  datatable(df,rownames = FALSE,options = list(pageLength = 10),colnames = c('Constituent','6 months cml.return','1 year cml.return','3 years cml.return')) %>%
    formatStyle(c('six_months_return','one_year_return','three_years_return'),
                backgroundColor = styleInterval(c(0,0.5),c('red','white','#1E8449')))}


#This function creates a datatable for EPS
EPS_table<-function(retrieved_data){
  df<-retrieved_data[complete.cases(retrieved_data), ]
  #df[df$Constituent=='adidas',c('Constituent')]='Adidas'
  df[df$Constituent=='thyssenkrupp',c('Constituent')]<-'Thyssenkrupp'
  df[df$Constituent=='Volkswagen (VW) vz',c('Constituent')]<-'Volkswagen'
  df<-df[order(df$Constituent),]
  #df[df$Constituent == 'Volkswagen (VW) vz',c('Constituent')]='Volkswagen'
  df[df$Constituent == 'Münchener Rückversicherungs-Gesellschaft',c('Constituent')]='Münchener RG'
  datatable(df,rownames = FALSE,options = list(pageLength = 5), colnames = c('Constituent','Current EPS value','EPS value last year'))%>% formatStyle(
    'Current_EPS',
    background = styleColorBar(df$`Current_EPS`, 'orange'),
    backgroundSize = '100% 90%',
    backgroundRepeat = 'no-repeat',
    backgroundPosition = 'center') %>%formatStyle(
      'EPS_last_year',
      background = styleColorBar(df$`EPS_last_year`, '#62B5F6'),
      backgroundSize = '100% 90%',
      backgroundRepeat = 'no-repeat',
      backgroundPosition = 'center')
  }


#Makes a horizontal bar datatable displaying PER
PER_table<-function(retrieved_data){
  df<-retrieved_data[complete.cases(retrieved_data), ]
  df[df$Constituent=='adidas',c('Constituent')]<-'Adidas'
  df[df$Constituent=='thyssenkrupp',c('Constituent')]<-'Thyssenkrupp'
  df[df$Constituent=='Volkswagen (VW) vz',c('Constituent')]<-'Volkswagen'
  df<-df[order(df$Constituent),]
  #df[df$Constituent == 'Volkswagen (VW) vz',c('Constituent')]='Volkswagen'
  df[df$Constituent == 'Münchener Rückversicherungs-Gesellschaft',c('Constituent')]='Münchener RG'
  datatable(df,rownames = FALSE,options = list(pageLength = 5), colnames = c('Constituent','Current PER value','PER value last year'))%>% formatStyle(
    'Current_PER',
    background = styleColorBar(df$`Current_PER`, 'orange'),
    backgroundSize = '100% 90%',
    backgroundRepeat = 'no-repeat',
    backgroundPosition = 'center') %>%formatStyle(
      'PER_last_year',
      background = styleColorBar(df$`PER_last_year`, '#62B5F6'),
      backgroundSize = '100% 90%',
      backgroundRepeat = 'no-repeat',
      backgroundPosition = 'center')
  
}


#This function creates a datatable for Profitability Ranking and Tags
rank_n_tag<-function(retrieved_data){
  df<-retrieved_data[,c('Profitability_rank','Constituent','Price_growth','Fundamental_growth')]
  df$Profitability_rank<- df$Profitability_rank+1
  df<-df[order(df$Profitability_rank),]
  df[df$Constituent=='adidas',c('Constituent')]<-'Adidas'
  df[df$Constituent=='thyssenkrupp',c('Constituent')]<-'Thyssenkrupp'
  df[df$Constituent == 'Volkswagen (VW) vz',c('Constituent')]='Volkswagen'
  df[df$Constituent == 'Münchener Rückversicherungs-Gesellschaft',c('Constituent')]='Münchener RG'
  datatable(df,rownames = FALSE,options = list(pageLength = 10),colnames = c('Rank','Constituent','Growth in stock price','Growth in fundamental')) %>%formatStyle('Profitability_rank', textAlign = 'center')
}