##This stores functions used for the Analysts Page of Analytics Dashboard.

analyst_rating_table<-function(retrieved_data){
  df<-retrieved_data[,c('Constituent','Analyst recommendation','Analyst rating')]
  df<-df[order(df$Constituent),]
  datatable(df,rownames = FALSE,options = list(pageLength = 10), colnames = c('Constituent','Recommendation','Rating')) %>% formatStyle(
  'Analyst rating',
  background = styleColorBar(retrieved_data$`Analyst rating`, 'steelblue'),
  backgroundSize = '100% 90%',
  backgroundRepeat = 'no-repeat',
  backgroundPosition = 'center')}


#Makes a table displaying target prices
analyst_target_prices<-function(retrieved_data){
  df<-retrieved_data[,c('Constituent','Median target price','Lowest target price','Highest target price')]
  df<-df[order(df$Constituent),]
  datatable(df,rownames = FALSE,options = list(pageLength = 10), colnames = c('Constituent','Median','Lowest','Highest'))  %>% formatCurrency(c('Median target price','Lowest target price','Highest target price'), '€')
}


#Makes a horizontal bar datatable displaying PER
PER_table<-function(retrieved_data){
df<-retrieved_data[,c('Constituent','Current PER','PER last year')]
df[df$Constituent=='adidas',c('Constituent')]<-'Adidas'
df<-df[order(df$Constituent),]
datatable(df,rownames = FALSE,options = list(pageLength = 5), colnames = c('Constituent','Current PER value','PER value last year'))%>% formatStyle(
  'Current PER',
  background = styleColorBar(df$`Current PER`, 'orange'),
  backgroundSize = '100% 90%',
  backgroundRepeat = 'no-repeat',
  backgroundPosition = 'center') %>%formatStyle(
    'PER last year',
    background = styleColorBar(df$`PER last year`, '#62B5F6'),
    backgroundSize = '100% 90%',
    backgroundRepeat = 'no-repeat',
    backgroundPosition = 'center')

#%>%
#formatStyle(c('Recent cross'),
#color = styleInterval(c('Golden Cross','Death Cross'),c('red')))
}