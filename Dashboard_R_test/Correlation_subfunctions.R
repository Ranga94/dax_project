correlation_news<-function(url_mongo,constituent){
  if(constituent=='Adidas'){
    corr <- mongo(collection = 'ADS_cor',
                  url = url_mongo,
                  verbose = FALSE, options = ssl_options())}
  
  if(constituent=='BMW'){
    corr <- mongo(collection = 'BMW_cor',
                  url = url_mongo,
                  verbose = FALSE, options = ssl_options())}
  if(constituent=='Deutsche Bank'){
    corr <- mongo(collection = 'DB_cor',
                  url = url_mongo,
                  verbose = FALSE, options = ssl_options())}
  if(constituent=='Commerzbank'){
    corr <- mongo(collection = 'CB_cor',
                  url =url_mongo,
                  verbose = FALSE, options = ssl_options())}
  if(constituent=='EON'){
    corr <- mongo(collection = 'EON_cor',
                  url = url_mongo,
                  verbose = FALSE, options = ssl_options())}
  
  corr = corr$find()
  if(constituent=='Adidas'){
    mydata <- corr[, c('News_sent','Close','Open','High','Low','date')]
  }else{mydata <- corr[, c('News_sent','Close','Open','High','Low','date')]}
  
  title_str = paste('Behavior of ',constituent, ' prices relative to News Sentiments',sep='' )
  
  Close = mydata$Close
  High = mydata$High
  Low = mydata$Low
  Open = mydata$Open
  News_sent = mydata$News_sent
  
  O <- cor(News_sent, Open)
  C <- cor(News_sent, Close)
  L <- cor(News_sent, Low)
  H <- cor(News_sent, High)
  
  p <- ggplot(mydata, aes(x = date, group=1))
  
  p <- p + {if (C > 0.5) geom_line(aes(y = Close), linetype = "dashed", colour = "black")}
  p <- p + {if (O > 0.5) geom_line(aes(y = Open), linetype = "dashed", colour = "red")}
  p <- p + {if (H > 0.5) geom_line(aes(y = High), linetype = "dashed", colour = "blue")}
  p <- p + {if (L > 0.5) geom_line(aes(y = Low), linetype = "dashed", colour = "orange")}
  
  
  ##add annotations and scaling
  if(constituent=='Adidas'){
    p <- p + geom_line(aes(y = News_sent*270, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.85, 0.950)) + labs(title = title_str) 
  }
  
  if(constituent=='BMW'){
    p <- p + geom_line(aes(y = News_sent*370, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str) 
  }
  
  if(constituent=='Commerzbank'){
    p <- p + geom_line(aes(y = News_sent*70, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str) 
  }
  
  if(constituent=='Deutsche Bank'){
    p <- p + geom_line(aes(y = News_sent*270, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str) 
  }
  
  if(constituent=='EON'){
    p <- p + geom_line(aes(y = News_sent*56, colour = "News Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title =title_str) 
  }
  p <- p + theme(plot.title=element_text(size=10))
  ggplotly(p)
  p_changed <- ggplotly(p)
  pp_changed=plotly_build(p_changed)   
  style( pp_changed ) %>% 
    layout( legend = list(x = 0.01, y = 0.95) )}


##This plots stock prices against Twitter Sentiment
correlation_twitter<-function(url_mongo,constituent){
  if(constituent=='Adidas'){
    corr <- mongo(collection = 'ADS_cor',
                  url = url_mongo,
                  verbose = FALSE, options = ssl_options())}
  
  if(constituent=='BMW'){
    corr <- mongo(collection = 'BMW_cor',
                  url =url_mongo,
                  verbose = FALSE, options = ssl_options())}
  if(constituent=='Deutsche Bank'){
    corr <- mongo(collection = 'DB_cor',
                  url = url_mongo,
                  verbose = FALSE, options = ssl_options())}
  if(constituent=='Commerzbank'){
    corr <- mongo(collection = 'CB_cor',
                  url = url_mongo,
                  verbose = FALSE, options = ssl_options())}
  if(constituent=='EON'){
    corr <- mongo(collection = 'EON_cor',
                  url = url_mongo,
                  verbose = FALSE, options = ssl_options())}
  
  corr = corr$find()
  if(constituent=='Adidas'){
    mydata <- corr[, c('Twitter_sent','Close','Open','High','Low','date')]
  }else{mydata <- corr[, c('Twitter_sent','Close','Open','High','Low','date')]}
  
  title_str = paste('Behavior of ',constituent, ' prices relative to Twitter Sentiments',sep='' )
  
  Close = mydata$Close
  High = mydata$High
  Low = mydata$Low
  Open = mydata$Open
  Twitter_sent = mydata$Twitter_sent
  
  O <- cor(Twitter_sent, Open)
  C <- cor(Twitter_sent, Close)
  L <- cor(Twitter_sent, Low)
  H <- cor(Twitter_sent, High)
  
  p <- ggplot(mydata, aes(x = date, group=1))
  
  p <- p + {if (C > 0.5) geom_line(aes(y = Close), linetype = "dashed", colour = "black")}
  p <- p + {if (O > 0.5) geom_line(aes(y = Open), linetype = "dashed", colour = "red")}
  p <- p + {if (H > 0.5) geom_line(aes(y = High), linetype = "dashed", colour = "blue")}
  p <- p + {if (L > 0.5) geom_line(aes(y = Low), linetype = "dashed", colour = "orange")}
  
  
  ##add annotations and scaling
  if(constituent=='Adidas'){
    p <- p + geom_line(aes(y = Twitter_sent*270, colour = "Twitter Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str ) 
  }
  
  if(constituent=='BMW'){
    p <- p + geom_line(aes(y = Twitter_sent*150, colour = "Twitter Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str ) 
  }
  
  if(constituent=='Commerzbank'){
    p <- p + geom_line(aes(y = Twitter_sent*40, colour = "Twitter Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str ) 
  }
  
  if(constituent=='Deutsche Bank'){
    p <- p + geom_line(aes(y = Twitter_sent*50, colour = "Twitter Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str ) 
  }
  
  if(constituent=='EON'){
    p <- p + geom_line(aes(y = Twitter_sent*12.5, colour = "Twitter Sentiment"))
    p <- p + theme(axis.title.y=element_blank(), legend.position = c(0.15, 0.850)) + labs(title = title_str ) 
  }
  p <- p + theme(plot.title=element_text(size=10))
  ggplotly(p)
  p_changed <- ggplotly(p)
  pp_changed=plotly_build(p_changed)   
  style( pp_changed ) %>% 
    layout( legend = list(x = 0.01, y = 0.95) )}


#This selects the annotation for News correlation graph
news_annotation_selection<-function(constituent){
  if(constituent=='Adidas'){
    str = "Correlations for all Adidas stock prices have less than 50% correlation to News sentiment"
  }
  if(constituent=='BMW'){
    str = "Open, Close, High and Low prices all have greater than 50% correlations to News sentiment. News sentiment had the greatest correlations to Highs, which had a 99% correlation, followed by Open and lows, which both were 97% correlated to sentiment, and finally, Close prices, which were 90% correlated."
  }
  if(constituent=='Commerzbank'){
    str = "Open, Close, High and Low prices all have greater than 50% correlations to News sentiment. News sentiment had the greatest correlations to Highs and Close Prices, which were both 99% correlated, followed by Lows, which was 91% correlated to sentiment, and finally, Open prices, which was 84% correlated."
  }
  if(constituent=='Deutsche Bank'){
    str = "Open, Close, High and Low prices all have greater than 50% correlations to News sentiment. News sentiment had the greatest correlations to Open and Close Prices, which were 93% correlated, followed by Lows, which was 90% correlated to sentiment, and finally, Highs, which was 71% correlated."
  }
  if(constituent=='EON'){
    str = "Only Close, Open and Lows have greater than 50% correlations to News sentiment. News sentiment had the greatest correlation to Lows, which had a 100% correlation, followed by Close, which was 93% correlated to sentiment, and finally, Open Prices, which were 54% correlated."
  }
  str
}

#This selects the annotation for Twitter correlation graph
twitter_annotation_selection<-function(constituent){
  if(constituent=='Adidas'){
    str = "Only Adidas open prices seem to have greater than 50% correlation to Twitter sentiment. Open prices have a 78% correlation to Twitter sentiment for the total time period shown."
  }
  if(constituent=='BMW'){
    str = "Open, Close, High and Low prices all have greater than 50% correlations to Twitter sentiment. Twitter sentiment had the greatest correlations to Close prices, which had a 99% correlation, followed by Open and lows, which both were 95% correlated to sentiment, and finally, Highs, which were 92% correlated."
  }
  if(constituent=='Commerzbank'){
    str = "Open, Close, High and Low prices all have greater than 50% correlations to Twitter sentiment. Twitter sentiment had the greatest correlation to Open Prices, which was 98% correlated, followed by Highs, which was 90% correlated to sentiment. Close prices were 89 percent correlated and Lows were 69% correlated."
  }
  if(constituent=='Deutsche Bank'){
    str = "Only Close, High and Low prices have greater than 50% correlations to Twitter sentiment. Twitter sentiment had the greatest correlations to Highs, which had a 98% correlation, followed by Low, which was 86% correlated to sentiment, and finally, Close Prices, which were 83% correlated."
  }
  if(constituent=='EON'){
    str = "Only Open and Highs have greater than 50% correlations to Twitter sentiment. Twitter sentiment had the greatest correlation to Open prices, which had a 99% correlation, while Highs were 94% correlated."
  }
  str
}
